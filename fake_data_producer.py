import csv
import random
from faker import Faker
from datetime import timedelta, datetime  

fake = Faker()

def generate_fake_orders(num_orders=1000):
    products = ['Laptop', 'Tablet', 'Smartphone', 'Headphones', 'Monitor', 'Keyboard']
    data = []

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 6, 30)

    for i in range(num_orders):
        added_to_cart_at = fake.date_time_between(start_date=start_date, end_date=end_date)

        if random.random() < 0.7:
            order_created_at = added_to_cart_at + timedelta(minutes=random.randint(5, 180))
            order_created_str = order_created_at.strftime('%Y-%m-%d %H:%M:%S')
            is_delivered = random.choice([True, False])
        else:
            order_created_str = ""
            is_delivered = False

        order = {
            "OrderID": i + 1,
            "UserID": fake.random_int(min=1000, max=9999),
            "AddedToCartAt": added_to_cart_at.strftime('%Y-%m-%d %H:%M:%S'),
            "OrderCreatedAt": order_created_str,
            "Amount": round(random.uniform(100, 2000), 2),
            "Product": random.choice(products),
            "IsDelivered": is_delivered
        }
        data.append(order)

    return data

def write_to_csv(filename, data):
    fieldnames = data[0].keys()
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    fake_data = generate_fake_orders()
    write_to_csv("orders.csv", fake_data)
    print("orders.csv başarıyla oluşturuldu.")
