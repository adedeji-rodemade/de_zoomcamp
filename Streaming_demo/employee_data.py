import csv
import random
from faker import Faker

# Initialize Faker library for generating random names
fake = Faker()

# Define CSV file name
csv_file = "employee_data.csv"

# Generate random employee data
def generate_employee_data(num_records=1000):
    with open(csv_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["EmployeeID", "FirstName", "Salary"])
        
        for emp_id in range(1, num_records + 1):
            first_name = fake.first_name()
            salary = round(random.uniform(30000, 150000), 2)  # Salary between 30k and 150k
            writer.writerow([emp_id, first_name, salary])

    print(f"CSV file '{csv_file}' with {num_records} records generated successfully!")

# Generate 1000 records
generate_employee_data()
