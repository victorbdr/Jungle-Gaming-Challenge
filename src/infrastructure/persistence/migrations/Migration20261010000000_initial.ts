import { Migration } from '@mikro-orm/migrations';

/**
 * Schema inicial. As invariantes financeiras são garantidas aqui, não só na aplicação:
 * - unicidade: wallet por (player, moeda), idempotency key, (provider, externalId), reversão por referência,
 *   lançamento por (transação, wallet), inbox por (consumer, messageId);
 * - não-negatividade: CHECK em saldo e em balance_before/after do ledger;
 * - imutabilidade: ledger append-only (UPDATE/DELETE/TRUNCATE bloqueados) e transações terminais congeladas;
 * - consistência saldo↔ledger: encadeamento balance_before = último balance_after e, no commit
 *   (constraint trigger DEFERRED), saldo da wallet = balance_after do último lançamento.
 */
export class Migration20261010000000_initial extends Migration {
  override up(): void {
    this.addSql(`
      create table wallets (
        id          uuid primary key,
        player_id   text not null check (length(player_id) between 1 and 128),
        currency    char(3) not null check (currency ~ '^[A-Z]{3}$'),
        balance     numeric(20,2) not null check (balance >= 0),
        version     integer not null check (version >= 1),
        created_at  timestamptz not null,
        updated_at  timestamptz not null,
        constraint wallets_player_currency_uq unique (player_id, currency)
      );
    `);

    this.addSql(`
      create table wager_transactions (
        id                                 uuid primary key,
        provider_id                        text not null check (length(provider_id) between 1 and 128),
        external_transaction_id            text not null check (length(external_transaction_id) between 1 and 256),
        idempotency_key                    text not null check (length(idempotency_key) between 1 and 512),
        payload_hash                       char(64) not null,
        -- sem FK: transações para wallet inexistente são persistidas como REJECTED (WALLET_NOT_FOUND) para replay idempotente
        wallet_id                          uuid not null,
        player_id                          text not null,
        round_id                           text not null,
        game_id                            text not null,
        kind                               text not null check (kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount                             numeric(20,2) not null check (amount >= 0),
        currency                           char(3) not null check (currency ~ '^[A-Z]{3}$'),
        reference_external_transaction_id  text,
        reference_transaction_id           uuid references wager_transactions(id),
        status                             text not null check (status in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code                       text,
        result_balance_amount              numeric(20,2),
        reference_attempts                 integer not null default 0 check (reference_attempts >= 0),
        next_reference_attempt_at          timestamptz,
        created_at                         timestamptz not null,
        processed_at                       timestamptz,
        constraint wager_tx_idempotency_key_uq unique (idempotency_key),
        constraint wager_tx_provider_external_uq unique (provider_id, external_transaction_id),
        constraint wager_tx_amount_positive_ck check (kind = 'LOSS' or amount > 0),
        constraint wager_tx_reference_required_ck check (kind not in ('REFUND','ROLLBACK') or reference_external_transaction_id is not null),
        constraint wager_tx_reference_forbidden_ck check (kind not in ('OPENING','BET','LOSS') or reference_external_transaction_id is null),
        constraint wager_tx_terminal_processed_at_ck check ((status in ('PROCESSED','REJECTED','FAILED')) = (processed_at is not null)),
        constraint wager_tx_failure_code_ck check ((status in ('REJECTED','FAILED')) = (failure_code is not null)),
        constraint wager_tx_pending_ref_schedule_ck check (status <> 'PENDING_REFERENCE' or next_reference_attempt_at is not null),
        constraint wager_tx_processed_reference_ck check (
          status <> 'PROCESSED' or reference_external_transaction_id is null or reference_transaction_id is not null
        )
      );
    `);
    // Uma referência só pode ser revertida uma vez (REFUND ou ROLLBACK), garantido no banco.
    this.addSql(`
      create unique index wager_tx_single_reversal_uq on wager_transactions (reference_transaction_id)
        where status = 'PROCESSED' and kind in ('REFUND','ROLLBACK');
    `);
    this.addSql(`
      create index wager_tx_pending_reference_due_idx on wager_transactions (next_reference_attempt_at)
        where status = 'PENDING_REFERENCE';
    `);
    this.addSql(`
      create index wager_tx_pending_reference_lookup_idx on wager_transactions (provider_id, reference_external_transaction_id)
        where status = 'PENDING_REFERENCE';
    `);
    this.addSql(`create index wager_tx_wallet_idx on wager_transactions (wallet_id, created_at);`);

    this.addSql(`
      create table wallet_ledger_entries (
        seq             bigint generated always as identity unique,
        id              uuid primary key,
        wallet_id       uuid not null references wallets(id),
        transaction_id  uuid not null references wager_transactions(id),
        direction       text not null check (direction in ('DEBIT','CREDIT')),
        amount          numeric(20,2) not null check (amount > 0),
        currency        char(3) not null,
        balance_before  numeric(20,2) not null check (balance_before >= 0),
        balance_after   numeric(20,2) not null check (balance_after >= 0),
        created_at      timestamptz not null,
        constraint ledger_tx_wallet_uq unique (transaction_id, wallet_id),
        constraint ledger_balanced_ck check (
          balance_after = case direction when 'CREDIT' then balance_before + amount else balance_before - amount end
        )
      );
    `);
    this.addSql(`create index ledger_wallet_seq_idx on wallet_ledger_entries (wallet_id, seq);`);

    // ---- imutabilidade do ledger
    this.addSql(`
      create function ledger_reject_mutation() returns trigger language plpgsql as $$
      begin
        raise exception 'wallet_ledger_entries is append-only (% blocked)', tg_op using errcode = 'P0001';
      end $$;
    `);
    this.addSql(`
      create trigger ledger_no_update_delete before update or delete on wallet_ledger_entries
        for each row execute function ledger_reject_mutation();
    `);
    this.addSql(`
      create trigger ledger_no_truncate before truncate on wallet_ledger_entries
        for each statement execute function ledger_reject_mutation();
    `);

    // ---- encadeamento: balance_before do novo lançamento = balance_after do anterior (0 se primeiro)
    this.addSql(`
      create function ledger_check_chain() returns trigger language plpgsql as $$
      declare
        prev numeric(20,2);
        wallet_currency char(3);
      begin
        select currency into wallet_currency from wallets where id = new.wallet_id;
        if wallet_currency is distinct from new.currency then
          raise exception 'ledger currency % differs from wallet currency %', new.currency, wallet_currency using errcode = '23514';
        end if;
        select balance_after into prev from wallet_ledger_entries
          where wallet_id = new.wallet_id order by seq desc limit 1;
        if new.balance_before <> coalesce(prev, 0) then
          raise exception 'ledger chain broken for wallet %: balance_before % <> previous balance_after %',
            new.wallet_id, new.balance_before, coalesce(prev, 0) using errcode = '23514';
        end if;
        return new;
      end $$;
    `);
    this.addSql(`
      create trigger ledger_chain_check before insert on wallet_ledger_entries
        for each row execute function ledger_check_chain();
    `);

    // ---- saldo materializado = último balance_after, verificado no commit
    this.addSql(`
      create function wallet_check_balance_matches_ledger() returns trigger language plpgsql as $$
      declare
        last_after numeric(20,2);
        current_balance numeric(20,2);
      begin
        select balance into current_balance from wallets where id = new.id;
        select balance_after into last_after from wallet_ledger_entries
          where wallet_id = new.id order by seq desc limit 1;
        if current_balance <> coalesce(last_after, 0) then
          raise exception 'wallet % balance % does not match ledger %', new.id, current_balance, coalesce(last_after, 0)
            using errcode = '23514';
        end if;
        return null;
      end $$;
    `);
    this.addSql(`
      create constraint trigger wallet_balance_matches_ledger
        after insert or update on wallets
        deferrable initially deferred
        for each row execute function wallet_check_balance_matches_ledger();
    `);
    this.addSql(`
      create function wallet_reject_delete() returns trigger language plpgsql as $$
      begin
        raise exception 'wallets cannot be deleted' using errcode = 'P0001';
      end $$;
    `);
    this.addSql(`
      create trigger wallet_no_delete before delete on wallets for each row execute function wallet_reject_delete();
    `);

    // ---- transações: campos de negócio imutáveis; estado terminal congelado; sem DELETE
    this.addSql(`
      create function wager_tx_guard() returns trigger language plpgsql as $$
      begin
        if tg_op = 'DELETE' then
          raise exception 'wager_transactions cannot be deleted' using errcode = 'P0001';
        end if;
        if old.status in ('PROCESSED','REJECTED','FAILED') then
          raise exception 'transaction % is terminal (%)', old.id, old.status using errcode = 'P0001';
        end if;
        if (new.provider_id, new.external_transaction_id, new.idempotency_key, new.payload_hash, new.wallet_id,
            new.player_id, new.round_id, new.game_id, new.kind, new.amount, new.currency,
            new.reference_external_transaction_id, new.created_at)
           is distinct from
           (old.provider_id, old.external_transaction_id, old.idempotency_key, old.payload_hash, old.wallet_id,
            old.player_id, old.round_id, old.game_id, old.kind, old.amount, old.currency,
            old.reference_external_transaction_id, old.created_at) then
          raise exception 'immutable fields of transaction % cannot change', old.id using errcode = 'P0001';
        end if;
        return new;
      end $$;
    `);
    this.addSql(`
      create trigger wager_tx_guard before update or delete on wager_transactions
        for each row execute function wager_tx_guard();
    `);

    this.addSql(`
      create table inbox_messages (
        consumer_name   text not null,
        message_id      text not null,
        payload_hash    char(64) not null,
        received_at     timestamptz not null,
        processed_at    timestamptz,
        primary key (consumer_name, message_id)
      );
    `);

    this.addSql(`
      create table outbox_messages (
        id               uuid primary key,
        aggregate_id     text not null,
        event_type       text not null,
        payload          jsonb not null,
        occurred_at      timestamptz not null,
        attempts         integer not null default 0 check (attempts >= 0),
        next_attempt_at  timestamptz not null,
        published_at     timestamptz,
        last_error       text
      );
    `);
    this.addSql(`
      create index outbox_pending_due_idx on outbox_messages (next_attempt_at) where published_at is null;
    `);
  }

  override down(): void {
    this.addSql('drop table if exists outbox_messages;');
    this.addSql('drop table if exists inbox_messages;');
    this.addSql('drop trigger if exists ledger_no_truncate on wallet_ledger_entries;');
    this.addSql('drop table if exists wallet_ledger_entries;');
    this.addSql('drop table if exists wager_transactions;');
    this.addSql('drop table if exists wallets;');
    this.addSql('drop function if exists ledger_reject_mutation();');
    this.addSql('drop function if exists ledger_check_chain();');
    this.addSql('drop function if exists wallet_check_balance_matches_ledger();');
    this.addSql('drop function if exists wallet_reject_delete();');
    this.addSql('drop function if exists wager_tx_guard();');
  }
}
