import { type CanActivate, type ExecutionContext, Injectable, SetMetadata } from '@nestjs/common';
import { Reflector } from '@nestjs/core';

/**
 * Ponto de extensão de autenticação (não implementada neste desafio — ver ARCHITECTURE.md §Autenticação).
 *
 * Desenho alvo: Keycloak (OIDC) emitindo tokens client_credentials por provedor; o guard valida o JWT
 * via JWKS (assinatura, iss, aud, exp) e um ProviderIdentityPort resolve `providerId` a partir do `azp`/claim.
 * O use case então exige que `providerId` do payload == provedor autenticado.
 */
export interface ProviderIdentity {
  providerId: string | null;
  subject: string;
}

export interface ProviderIdentityPort {
  resolve(authorizationHeader: string | undefined): Promise<ProviderIdentity>;
}

/** Implementação no-op: aceita qualquer requisição e não restringe o provider. */
export class AnonymousProviderIdentity implements ProviderIdentityPort {
  async resolve(): Promise<ProviderIdentity> {
    return { providerId: null, subject: 'anonymous' };
  }
}

export const PUBLIC_ROUTE = 'isPublicRoute';
export const Public = () => SetMetadata(PUBLIC_ROUTE, true);

@Injectable()
export class AuthGuard implements CanActivate {
  private readonly identity: ProviderIdentityPort = new AnonymousProviderIdentity();

  constructor(private readonly reflector: Reflector) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(PUBLIC_ROUTE, [context.getHandler(), context.getClass()]);
    if (isPublic) return true;
    const req = context.switchToHttp().getRequest<{ headers: Record<string, string | undefined>; identity?: ProviderIdentity }>();
    req.identity = await this.identity.resolve(req.headers.authorization);
    return true;
  }
}
