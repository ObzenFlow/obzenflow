use super::domain::*;

pub fn categories() -> Vec<Category> {
    vec![
        Category {
            category_id: "ELEC".to_string(),
            category_name: "Electronics".to_string(),
            department: "Technology".to_string(),
            margin_target: 0.25,
        },
        Category {
            category_id: "HOME".to_string(),
            category_name: "Home & Garden".to_string(),
            department: "Living".to_string(),
            margin_target: 0.30,
        },
        Category {
            category_id: "BOOK".to_string(),
            category_name: "Books".to_string(),
            department: "Media".to_string(),
            margin_target: 0.40,
        },
    ]
}

pub fn products() -> Vec<Product> {
    vec![
        Product {
            product_id: "LAPTOP-PRO".to_string(),
            product_name: "Gaming Laptop Pro".to_string(),
            category_id: "ELEC".to_string(),
            brand: "TechBrand".to_string(),
            base_price: 1299.99,
        },
        Product {
            product_id: "LAPTOP-BUS".to_string(),
            product_name: "Business Laptop".to_string(),
            category_id: "ELEC".to_string(),
            brand: "OfficeTech".to_string(),
            base_price: 999.99,
        },
        Product {
            product_id: "PHONE-SMART".to_string(),
            product_name: "Smartphone X".to_string(),
            category_id: "ELEC".to_string(),
            brand: "PhoneCorp".to_string(),
            base_price: 899.99,
        },
        Product {
            product_id: "CHAIR-ERGO".to_string(),
            product_name: "Ergonomic Office Chair".to_string(),
            category_id: "HOME".to_string(),
            brand: "ComfortPlus".to_string(),
            base_price: 299.99,
        },
        Product {
            product_id: "BOOK-RUST".to_string(),
            product_name: "Rust Programming Guide".to_string(),
            category_id: "BOOK".to_string(),
            brand: "TechBooks".to_string(),
            base_price: 45.99,
        },
    ]
}

pub fn skus() -> Vec<SKU> {
    vec![
        SKU {
            sku_id: "LAPTOP-PRO-16-512".to_string(),
            product_id: "LAPTOP-PRO".to_string(),
            variant: "16GB/512GB SSD".to_string(),
            unit_cost: 900.00,
            current_price: 1299.99,
        },
        SKU {
            sku_id: "LAPTOP-PRO-32-1TB".to_string(),
            product_id: "LAPTOP-PRO".to_string(),
            variant: "32GB/1TB SSD".to_string(),
            unit_cost: 1100.00,
            current_price: 1599.99,
        },
        SKU {
            sku_id: "LAPTOP-BUS-8-256".to_string(),
            product_id: "LAPTOP-BUS".to_string(),
            variant: "8GB/256GB SSD".to_string(),
            unit_cost: 700.00,
            current_price: 999.99,
        },
        SKU {
            sku_id: "PHONE-SMART-128".to_string(),
            product_id: "PHONE-SMART".to_string(),
            variant: "128GB".to_string(),
            unit_cost: 600.00,
            current_price: 899.99,
        },
        SKU {
            sku_id: "CHAIR-ERGO-BLACK".to_string(),
            product_id: "CHAIR-ERGO".to_string(),
            variant: "Black".to_string(),
            unit_cost: 180.00,
            current_price: 299.99,
        },
        SKU {
            sku_id: "BOOK-RUST-PAPER".to_string(),
            product_id: "BOOK-RUST".to_string(),
            variant: "Paperback".to_string(),
            unit_cost: 20.00,
            current_price: 45.99,
        },
    ]
}

pub fn promotions() -> Vec<Promotion> {
    vec![
        Promotion {
            sku_id: "LAPTOP-PRO-16-512".to_string(),
            promo_code: "FLASH15".to_string(),
            discount_pct: 0.15,
            promo_type: "Flash Sale".to_string(),
        },
        Promotion {
            sku_id: "PHONE-SMART-128".to_string(),
            promo_code: "MOBILE10".to_string(),
            discount_pct: 0.10,
            promo_type: "Mobile Monday".to_string(),
        },
        // Note: other SKUs have no promotions.
    ]
}

pub fn payments() -> Vec<PaymentMethod> {
    vec![
        PaymentMethod {
            payment_id: "PM-001".to_string(),
            card_type: "Visa".to_string(),
            risk_score: 0.02,
            approved: true,
        },
        PaymentMethod {
            payment_id: "PM-002".to_string(),
            card_type: "Mastercard".to_string(),
            risk_score: 0.03,
            approved: true,
        },
        PaymentMethod {
            payment_id: "PM-003".to_string(),
            card_type: "Amex".to_string(),
            risk_score: 0.01,
            approved: true,
        },
        // Note: PM-004 intentionally missing to demonstrate StrictJoin failure path.
    ]
}

pub fn orders(inject_bad_payment: bool) -> Vec<OrderEvent> {
    let mut orders = vec![
        OrderEvent {
            order_id: "ORD-001".to_string(),
            sku_id: "LAPTOP-PRO-16-512".to_string(),
            quantity: 1,
            payment_id: "PM-001".to_string(),
            timestamp: 1000,
        },
        OrderEvent {
            order_id: "ORD-002".to_string(),
            sku_id: "PHONE-SMART-128".to_string(),
            quantity: 2,
            payment_id: "PM-002".to_string(),
            timestamp: 1001,
        },
        OrderEvent {
            order_id: "ORD-003".to_string(),
            sku_id: "BOOK-RUST-PAPER".to_string(),
            quantity: 3,
            payment_id: "PM-003".to_string(),
            timestamp: 1002,
        },
        OrderEvent {
            order_id: "ORD-004".to_string(),
            sku_id: "LAPTOP-PRO-32-1TB".to_string(),
            quantity: 1,
            payment_id: "PM-001".to_string(),
            timestamp: 1003,
        },
        OrderEvent {
            order_id: "ORD-005".to_string(),
            sku_id: "CHAIR-ERGO-BLACK".to_string(),
            quantity: 2,
            payment_id: "PM-002".to_string(),
            timestamp: 1004,
        },
    ];

    if inject_bad_payment {
        orders.push(OrderEvent {
            order_id: "ORD-666".to_string(),
            sku_id: "LAPTOP-PRO-16-512".to_string(),
            quantity: 1,
            payment_id: "PM-004".to_string(), // does not exist
            timestamp: 1005,
        });
    }

    orders
}
