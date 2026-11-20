package sqala.spring.boot

import sqala.jdbc.Logger

/**
 * Spring-managed logging holder. Inject it into a service and import
 * its given logger with `import logging.given`.
 */
final class SqalaLogging(val logger: Logger):
    given sqalaLogger: Logger = logger