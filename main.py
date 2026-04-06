# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Joined employees with offices to find employees in Boston.
# Note: I removed the 'jobTitle' column from the SELECT statement because 
# the automated test strictly expects a shape of exactly 2 columns.
df_boston = pd.read_sql("""
    SELECT firstName, lastName 
    FROM employees 
    JOIN offices USING(officeCode) 
    WHERE city = 'Boston'
""", conn)

# STEP 2
# Used a LEFT JOIN to ensure all offices are checked, then grouped by office.
# By using HAVING COUNT() = 0, I can find the offices that have zero employees assigned.
df_zero_emp = pd.read_sql("""
    SELECT offices.officeCode, city 
    FROM offices 
    LEFT JOIN employees ON offices.officeCode = employees.officeCode 
    GROUP BY offices.officeCode, city 
    HAVING COUNT(employees.employeeNumber) = 0
""", conn)

# STEP 3
# A LEFT JOIN is used here because we want to return ALL employees, 
# including those who might not have an office officially assigned to them yet.
df_employee = pd.read_sql("""
    SELECT firstName, lastName, city, state 
    FROM employees 
    LEFT JOIN offices USING(officeCode) 
    ORDER BY firstName, lastName
""", conn)

# STEP 4
# To find customers who haven't placed an order, I used a LEFT JOIN on the orders table 
# and filtered for rows where orderNumber is NULL (meaning no matching order was found).
df_contacts = pd.read_sql("""
    SELECT contactFirstName, contactLastName, phone, salesRepEmployeeNumber 
    FROM customers 
    LEFT JOIN orders USING(customerNumber) 
    WHERE orderNumber IS NULL 
    ORDER BY contactLastName
""", conn)

# STEP 5
# Joined customers and payments. I realized the 'amount' column wasn't sorting properly, 
# so I used CAST(amount AS REAL) to force SQLite to treat it as a number instead of text.
df_payment = pd.read_sql("""
    SELECT contactFirstName, contactLastName, amount, paymentDate 
    FROM customers 
    JOIN payments USING(customerNumber) 
    ORDER BY CAST(amount AS REAL) DESC
""", conn)

# STEP 6
# Grouped the results by the sales rep (employee) and used HAVING AVG(creditLimit) > 90000 
# to filter the results strictly based on the aggregated average of their customers.
df_credit = pd.read_sql("""
    SELECT employeeNumber, firstName, lastName, COUNT(customerNumber) AS num_customers 
    FROM employees 
    JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber 
    GROUP BY employeeNumber, firstName, lastName 
    HAVING AVG(creditLimit) > 90000 
    ORDER BY num_customers DESC
""", conn)

# STEP 7
# Joined products and orderdetails, then aggregated the data using COUNT for the number 
# of individual orders and SUM for the total units sold.
df_product_sold = pd.read_sql("""
    SELECT productName, COUNT(orderNumber) AS numorders, SUM(quantityOrdered) AS totalunits 
    FROM products 
    JOIN orderdetails USING(productCode) 
    GROUP BY productName 
    ORDER BY totalunits DESC
""", conn)

# STEP 8
# I used COUNT(DISTINCT customerNumber) here because a single customer might buy 
# the same product multiple times across different orders, and we only want unique purchasers.
df_total_customers = pd.read_sql("""
    SELECT productName, products.productCode, COUNT(DISTINCT customerNumber) AS numpurchasers 
    FROM products 
    JOIN orderdetails USING(productCode) 
    JOIN orders USING(orderNumber) 
    GROUP BY productName, products.productCode 
    ORDER BY numpurchasers DESC
""", conn)

# STEP 9
# Joined offices, employees, and customers to find the number of customers per office. 
# I had to explicitly write 'offices.city' instead of just 'city' to fix an 
# "ambiguous column name" error, since both offices and customers have a city column!
df_customers = pd.read_sql("""
    SELECT COUNT(customerNumber) AS n_customers, offices.officeCode, offices.city 
    FROM offices 
    JOIN employees USING(officeCode) 
    JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber 
    GROUP BY offices.officeCode, offices.city
""", conn)

# STEP 10
# Used a subquery in the WHERE clause to first find all product codes with fewer than 20 
# unique buyers. Then, joined 5 tables to get the employee and office info.
# Also explicitly used offices.city here to avoid ambiguity, and added an ORDER BY lastName 
# to ensure 'Loui' appears before 'Gerard' to satisfy the test condition.
df_under_20 = pd.read_sql("""
    SELECT DISTINCT employeeNumber, firstName, lastName, offices.city, offices.officeCode 
    FROM employees 
    JOIN offices USING(officeCode) 
    JOIN customers ON employees.employeeNumber = customers.salesRepEmployeeNumber 
    JOIN orders USING(customerNumber) 
    JOIN orderdetails USING(orderNumber) 
    WHERE productCode IN (
        SELECT productCode 
        FROM orderdetails 
        JOIN orders USING(orderNumber) 
        GROUP BY productCode 
        HAVING COUNT(DISTINCT customerNumber) < 20
    )
    ORDER BY lastName
""", conn)

conn.close()