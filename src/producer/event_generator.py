from src.config import (DATABASE_SERVER, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD, SCHEMA)
from fastapi import FastAPI
import random
import uuid

app = FastAPI()

PRODUCTS = [
    # SHAMPOOS (1-7)
    {"id": 1, "name": "Coconut Curl Hydrating Shampoo", "category": "shampoo", "price": 14.99, "stock": 120},
    {"id": 2, "name": "Shea Butter Moisture Shampoo", "category": "shampoo", "price": 16.50, "stock": 95},
    {"id": 3, "name": "Aloe Vera Curl Cleanser", "category": "shampoo", "price": 13.99, "stock": 140},
    {"id": 4, "name": "Clarifying Curl Detox Shampoo", "category": "shampoo", "price": 18.00, "stock": 75},
    {"id": 5, "name": "Rosemary Scalp Shampoo", "category": "shampoo", "price": 17.50, "stock": 85},
    {"id": 6, "name": "Avocado Nourishing Shampoo", "category": "shampoo", "price": 15.99, "stock": 110},
    {"id": 7, "name": "Low-Poo Gentle Curl Wash", "category": "shampoo", "price": 19.99, "stock": 65},

    # CONDITIONERS (8-14)
    {"id": 8, "name": "Coconut Milk Curl Conditioner", "category": "conditioner", "price": 15.99, "stock": 130},
    {"id": 9, "name": "Shea Butter Deep Conditioner", "category": "conditioner", "price": 18.50, "stock": 100},
    {"id": 10, "name": "Argan Oil Repair Conditioner", "category": "conditioner", "price": 17.99, "stock": 90},
    {"id": 11, "name": "Aloe Vera Detangling Conditioner", "category": "conditioner", "price": 14.50, "stock": 145},
    {"id": 12, "name": "Protein Strengthening Conditioner", "category": "conditioner", "price": 21.00, "stock": 70},
    {"id": 13, "name": "Mango Butter Curl Conditioner", "category": "conditioner", "price": 16.99, "stock": 115},
    {"id": 14, "name": "Co-Wash Cleansing Conditioner", "category": "conditioner", "price": 19.50, "stock": 80},

    # CURL CREAMS (15-21)
    {"id": 15, "name": "Ultra Definition Curl Cream", "category": "curl_cream", "price": 22.99, "stock": 125},
    {"id": 16, "name": "Coconut Curl Enhancing Cream", "category": "curl_cream", "price": 19.99, "stock": 150},
    {"id": 17, "name": "Shea Butter Styling Cream", "category": "curl_cream", "price": 20.50, "stock": 110},
    {"id": 18, "name": "Lightweight Wavy Hair Cream", "category": "curl_cream", "price": 18.99, "stock": 100},
    {"id": 19, "name": "Anti-Frizz Curl Defining Cream", "category": "curl_cream", "price": 24.00, "stock": 95},
    {"id": 20, "name": "Avocado Curl Sculpting Cream", "category": "curl_cream", "price": 21.50, "stock": 85},
    {"id": 21, "name": "Maximum Moisture Curl Butter", "category": "curl_cream", "price": 25.99, "stock": 65},

    # STYLING GELS AND MOUSSES (22-29)
    {"id": 22, "name": "Strong Hold Curl Gel", "category": "styling_gel", "price": 16.99, "stock": 160},
    {"id": 23, "name": "Flaxseed Curl Definition Gel", "category": "styling_gel", "price": 18.50, "stock": 125},
    {"id": 24, "name": "Flexible Hold Styling Gel", "category": "styling_gel", "price": 15.99, "stock": 135},
    {"id": 25, "name": "Humidity Resistant Curl Gel", "category": "styling_gel", "price": 23.50, "stock": 90},
    {"id": 26, "name": "Aloe Vera Natural Curl Gel", "category": "styling_gel", "price": 17.00, "stock": 115},
    {"id": 27, "name": "Volumizing Curl Mousse", "category": "mousse", "price": 19.99, "stock": 105},
    {"id": 28, "name": "Lightweight Wave Styling Mousse", "category": "mousse", "price": 18.99, "stock": 95},
    {"id": 29, "name": "Extra Hold Curl Foam", "category": "mousse", "price": 21.00, "stock": 80},

    # LEAVE-IN CONDITIONERS (30-35)
    {"id": 30, "name": "Daily Hydration Leave-In", "category": "leave_in", "price": 17.99, "stock": 170},
    {"id": 31, "name": "Coconut Milk Leave-In Spray", "category": "leave_in", "price": 16.50, "stock": 135},
    {"id": 32, "name": "Deep Moisture Leave-In Cream", "category": "leave_in", "price": 22.00, "stock": 110},
    {"id": 33, "name": "Heat Protection Curl Spray", "category": "leave_in", "price": 20.99, "stock": 95},
    {"id": 34, "name": "Detangling Curl Mist", "category": "leave_in", "price": 14.99, "stock": 150},
    {"id": 35, "name": "Curl Refreshing Spray", "category": "leave_in", "price": 18.50, "stock": 125},

    # HAIR MASKS AND TREATMENTS (36-40)
    {"id": 36, "name": "Intensive Hydration Hair Mask", "category": "hair_mask", "price": 28.99, "stock": 75},
    {"id": 37, "name": "Protein Repair Hair Mask", "category": "hair_mask", "price": 32.00, "stock": 60},
    {"id": 38, "name": "Avocado Deep Treatment Mask", "category": "hair_mask", "price": 26.50, "stock": 85},
    {"id": 39, "name": "Overnight Curl Recovery Mask", "category": "hair_mask", "price": 35.99, "stock": 45},
    {"id": 40, "name": "Shea Butter Moisture Treatment", "category": "hair_mask", "price": 24.99, "stock": 100},

    # OILS AND SERUMS (41-44)
    {"id": 41, "name": "Pure Argan Hair Oil", "category": "hair_oil", "price": 24.99, "stock": 80},
    {"id": 42, "name": "Jojoba Scalp Nourishing Oil", "category": "hair_oil", "price": 21.50, "stock": 95},
    {"id": 43, "name": "Anti-Frizz Shine Serum", "category": "serum", "price": 27.99, "stock": 70},
    {"id": 44, "name": "Rosemary Scalp Serum", "category": "serum", "price": 29.50, "stock": 65},

    # ACCESSORIES AND TOOLS (45-50)
    {"id": 45, "name": "Microfiber Curl Drying Towel", "category": "accessory", "price": 15.99, "stock": 200},
    {"id": 46, "name": "Satin Hair Bonnet", "category": "accessory", "price": 18.99, "stock": 140},
    {"id": 47, "name": "Wide Tooth Detangling Comb", "category": "accessory", "price": 9.99, "stock": 250},
    {"id": 48, "name": "Curl Defining Styling Brush", "category": "accessory", "price": 22.50, "stock": 115},
    {"id": 49, "name": "Universal Hair Diffuser", "category": "tool", "price": 39.99, "stock": 55},
    {"id": 50, "name": "Satin Pillowcase for Curly Hair", "category": "accessory", "price": 25.99, "stock": 100},
]

STORES = [
    {"id": 1, "name": "Curl Paradise", "city": "New York", "country": "USA", "currency": "USD"},
    {"id": 2, "name": "Natural Curls Boutique", "city": "Los Angeles", "country": "USA", "currency": "USD"},
    {"id": 3, "name": "The Curly Shop", "city": "Miami", "country": "USA", "currency": "USD"},
    {"id": 4, "name": "Curl & Co.", "city": "Chicago", "country": "USA", "currency": "USD"},
    {"id": 5, "name": "Wavy Hair Essentials", "city": "Houston", "country": "USA", "currency": "USD"},
    {"id": 6, "name": "Curls & Confidence", "city": "San Francisco", "country": "USA", "currency": "USD"},
    {"id": 7, "name": "Defined Curls Studio", "city": "Seattle", "country": "USA", "currency": "USD"},
    {"id": 8, "name": "Curly Crown Beauty", "city": "Atlanta", "country": "USA", "currency": "USD"},
    {"id": 9, "name": "The Curl Lab", "city": "Boston", "country": "USA", "currency": "USD"},
    {"id": 10, "name": "Pure Curl Collective", "city": "Austin", "country": "USA", "currency": "USD"},
]


def _producer_sale() -> dict:
    store = random.choice(STORES)
    product = random.choice(PRODUCTS)


    order = {
        "order_id": str(uuid.uuid4()),
        "store_id": store["id"],
        "store_name": store["name"],
        "store_city": store["city"],
        "store_country": store["country"],
        "store_currency": store["currency"],
        "product_id": product["id"],
        "product_name": product["name"],
        "product_category": product["category"],
        "product_price": product["price"],
        "product_stock": product["stock"],
        "quantity": random.randint(1, 5),
        "status": "completed"
    }
    order['total_price'] = round(order['product_price'] * order['quantity'], 2)
    order['order_date'] = f"2026-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
    return order

def _producer_refund() -> dict:
    from mssql_python import connect, OperationalError, ConnectionStringParseError

    try:
        with connect(server=DATABASE_SERVER, database=DATABASE_NAME, uid=DATABASE_USER,
                    pwd=DATABASE_PASSWORD, encrypt="yes", trust_server_certificate="no",
                    timeout=60) as conn:
            with conn.cursor() as cursor:
                schema = SCHEMA.replace("]", "]]")
                cursor.execute(f"SELECT TOP 10 order_id, total_price FROM [{schema}].orders "
                               "WHERE status = 'completed' ORDER BY NEWID()")
                completed_orders = cursor.fetchall()
                if not completed_orders:
                    return _producer_sale()
                selected_order = random.choice(completed_orders)
                selected_id, selected_price = str(selected_order[0]), float(selected_order[1])

        n = random.random()
        refund_amount = round(selected_price / 2, 2)

        if n < 0.4:
            refund_amount = round(selected_price * random.random(), 2)

        return {
            "refund_id": str(uuid.uuid4()),
            "order_id": selected_id,
            "refund_amount": refund_amount,
            "refunded_at": f"2026-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "reason": "Customer requested refund" if n < 0.5 else "Product defect",
        }

    except (OperationalError, ConnectionStringParseError) as e:
        return _producer_sale() # Most likely case is that the database is empty, so we will just produce a sale event instead.
    



@app.get("/")
def produce_order() -> dict:
    n = random.random()

    if n <= 0.85:
        return _producer_sale()
    else:
        return _producer_refund()


    



