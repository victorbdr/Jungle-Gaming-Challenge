/**
 * Taxonomia estável de falhas, legível por máquina. Ver ARCHITECTURE.md §Failure codes.
 * Todas são terminais para o payload enviado: reenviar o mesmo payload devolve o mesmo resultado.
 */
export enum FailureCode {
  // saldo
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  ReversalWouldOverdraw = 'REVERSAL_WOULD_OVERDRAW',
  // validação contra a wallet
  WalletNotFound = 'WALLET_NOT_FOUND',
  WalletPlayerMismatch = 'WALLET_PLAYER_MISMATCH',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  // referências
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  ReferenceKindNotAllowed = 'REFERENCE_KIND_NOT_ALLOWED',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  ReversalAmountMismatch = 'REVERSAL_AMOUNT_MISMATCH',
  // infraestrutura (status FAILED)
  ProcessingFailed = 'PROCESSING_FAILED',
}
