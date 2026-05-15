import { useMemo } from 'react';
import { SystemRoles } from '@because/data-provider';
import { useAuthContext } from '~/hooks';

/**
 * Centralised authorisation gate for the pipeline feature surface (route
 * guard + nav-menu item).
 *
 * Today the only privileged role recognised by `@because/data-provider` is
 * `ADMIN`. Once the package is rebuilt with a `FINANCE` member, change the
 * implementation below to:
 *
 *     return role === SystemRoles.ADMIN || role === SystemRoles.FINANCE;
 *
 * Every consumer (`PipelineManager`, `AccountSettings`, future API guards)
 * imports this hook so the rollout is a single edit + build.
 *
 * Tracking checklist lives at:
 *   pipeline-svc/docs/finance-role-rollout.md
 */
export function usePipelineAccess(): {
  hasAccess: boolean;
  role: string | undefined;
  /** True only while the auth context is still loading the role. */
  isLoading: boolean;
} {
  const { user, isAuthenticated } = useAuthContext();
  return useMemo(() => {
    const role = user?.role;
    const finance = (SystemRoles as Record<string, string>).FINANCE;
    const hasAccess =
      role === SystemRoles.ADMIN || (finance ? role === finance : false);
    return {
      hasAccess,
      role,
      isLoading: !!isAuthenticated && !user,
    };
  }, [user, isAuthenticated]);
}
