const { SystemRoles } = require('@because/data-provider');
const { logger } = require('@because/data-schemas');

/**
 * Express middleware factory: ensures `req.user.role` is one of `allowedRoles`.
 *
 * Usage:
 *   router.use(requireRoles([SystemRoles.ADMIN]));
 *   router.use(requireRoles([SystemRoles.ADMIN, 'FINANCE']));   // forward-compat
 *
 * NOTE: SystemRoles currently only has ADMIN/USER. The pipeline plan introduces a
 * `finance` role; until it is added to `@because/data-provider` (Phase 6/10), pass
 * the literal string 'FINANCE' if you want to allow it.
 */
function requireRoles(allowedRoles = [SystemRoles.ADMIN]) {
  const allowSet = new Set(allowedRoles);
  return function checkRoles(req, res, next) {
    try {
      if (!req.user) {
        logger.warn('[requireRoles] Unauthenticated request');
        return res.status(401).json({ message: 'Unauthorized' });
      }
      if (!allowSet.has(req.user.role)) {
        logger.warn(
          `[requireRoles] User ${req.user.id} role=${req.user.role} not in [${[...allowSet].join(',')}]`,
        );
        return res.status(403).json({ message: 'Forbidden' });
      }
      next();
    } catch (error) {
      logger.error('[requireRoles] Error checking roles:', error);
      return res.status(500).json({ message: 'Internal Server Error' });
    }
  };
}

module.exports = requireRoles;
