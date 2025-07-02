//! Unit tests for middleware factory implementation

#[cfg(test)]
mod tests {
    use obzenflow_adapters::middleware::MiddlewareFactory;
    use obzenflow_adapters::monitoring::taxonomies::{red::RED, use_taxonomy::USE, golden_signals::GoldenSignals, saafe::SAAFE};

    #[test]
    fn test_red_factory_name() {
        let factory = RED::monitoring();
        assert_eq!(factory.name(), "RED::monitoring");
    }

    #[test]
    fn test_use_factory_name() {
        let factory = USE::monitoring();
        assert_eq!(factory.name(), "USE::monitoring");
    }

    #[test]
    fn test_golden_signals_factory_name() {
        let factory = GoldenSignals::monitoring();
        assert_eq!(factory.name(), "GoldenSignals::monitoring");
    }

    #[test]
    fn test_saafe_factory_name() {
        let factory = SAAFE::monitoring();
        assert_eq!(factory.name(), "SAAFE::monitoring");
    }

    #[test]
    fn test_all_factories_are_send_sync() {
        // This test verifies that all factories can be sent between threads
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        
        assert_send_sync(&RED::monitoring());
        assert_send_sync(&USE::monitoring());
        assert_send_sync(&GoldenSignals::monitoring());
        assert_send_sync(&SAAFE::monitoring());
    }
}