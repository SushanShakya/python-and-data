import sqlite3
from tabulate import tabulate
from functools import reduce

conn = sqlite3.connect('report.db')

cur = conn.cursor()

years = cur.execute('SELECT year FROM YEAR ORDER BY year;')

availYears = []

for row in years:
    availYears.append(row[0])

startEven = availYears[0] % 2 == 0;

data = conn.execute('SELECT B.year as year, C.name as product, D.name as country, A.sales FROM SALES AS A JOIN YEAR AS B ON A.year_id = B.id JOIN PRODUCT AS C ON A.product_id = C.id JOIN COUNTRY AS D on A.country_id = D.id ORDER BY c.name, b.year')

formatted = {}

for i in data:
    product = i[1]
    year = i[0]
    if startEven:
        if year % 2 != 0:
            year -= 1
    else:
        if year % 2 == 0:
            year -= 1

    sales = i[3]

    try:
        formatted[product]
        try:
            formatted[product][year]
            formatted[product][year] = [*formatted[product][year], sales]
        except:
            formatted[product][year] = [sales]

    except:
        formatted[product] = {
                year: [sales]
        }

finalData = []

for j in formatted:
    productName = j
    for i in formatted[j]:
        years = f"{i}-{i + 1}"
        sales = formatted[j][i]
        avg = reduce(lambda a,b: a+b,sales ) / len(sales)

        finalData.append([productName, years, avg])

print("\n\nAverage Sale for 2 years interval\n")
print(tabulate(finalData, headers=['Product','Year','Avg.'], tablefmt='orgtbl'))


conn.close()

