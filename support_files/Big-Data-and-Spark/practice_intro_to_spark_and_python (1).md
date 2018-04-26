
# Intro to Spark and Python

## - Topic - RDD Transformations and Actions

* Think of Transformations a recipes that are create but not executed until we say
* Think of Actions as the execution of recipes
* This process helps to increase efficiency and speed within Spark

### Step 1 - Importing Spark


```python
# Import sparkcontext from pyspark

from pyspark import SparkContext
```


```python
# Create the connection to the spark cluster

sc = SparkContext()
```

### Step 2 - Create Text File to work with


```python
# Create a text file
```


```python
%%writefile example2.txt
first line
second line
the third line
then a fourth line
```

    Overwriting example2.txt



```python
# Create RDD of text file

textFile = sc.textFile('example2.txt')
```


```python
# Alter the name of the variable

text_rdd = textFile
```


```python
# Conduct a Transformation on the object
# map() -> applies a function to all elements in the list
# lambda -> allows us to use anonymous functions
# split -> splits on spaces

words = text_rdd.map(lambda line: line.split())
```


```python
# Conduct Action on the object

words.collect()
```




    [['first', 'line'],
     ['second', 'line'],
     ['the', 'third', 'line'],
     ['then', 'a', 'fourth', 'line']]



### Step 3 - Map vs. Flat Map


```python
# Coduct the flatmap method on the RDD

text_rdd.flatMap(lambda line: line.split()).collect()
```




    ['first',
     'line',
     'second',
     'line',
     'the',
     'third',
     'line',
     'then',
     'a',
     'fourth',
     'line']



### Step 4 - Working with Key Value Pair Data


```python
%%writefile services.txt
#EventId    Timestamp    Customer   State    ServiceID    Amount
201       10/13/2017      100       NY       131          100.00
204       10/18/2017      700       TX       129          450.00
202       10/15/2017      203       CA       121          200.00
206       10/19/2017      202       CA       131          500.00
203       10/17/2017      101       NY       173          750.00
205       10/19/2017      202       TX       121          200.00
```

    Overwriting services.txt



```python
# Create a RDD object from the text file

services = sc.textFile('services.txt')
```


```python
# Use the .take() method to display the first 2 elements

services.take(2)
```




    ['#EventId    Timestamp    Customer   State    ServiceID    Amount',
     '201       10/13/2017      100       NY       131          100.00']




```python
# Use the .map() method to split up lines and display top 3 elements
## Note: we now have a list of items

services.map(lambda line: line.split()).take(3)
```




    [['#EventId', 'Timestamp', 'Customer', 'State', 'ServiceID', 'Amount'],
     ['201', '10/13/2017', '100', 'NY', '131', '100.00'],
     ['204', '10/18/2017', '700', 'TX', '129', '450.00']]




```python
# Remove # tag from header string

clean = services.map(lambda line: line[1:] if line[0] == '#' else line)
```


```python
# Split the object elements into separate lists

clean = clean.map(lambda line: line.split())
```


```python
# Display returns

clean.collect()
```




    [['EventId', 'Timestamp', 'Customer', 'State', 'ServiceID', 'Amount'],
     ['201', '10/13/2017', '100', 'NY', '131', '100.00'],
     ['204', '10/18/2017', '700', 'TX', '129', '450.00'],
     ['202', '10/15/2017', '203', 'CA', '121', '200.00'],
     ['206', '10/19/2017', '202', 'CA', '131', '500.00'],
     ['203', '10/17/2017', '101', 'NY', '173', '750.00'],
     ['205', '10/19/2017', '202', 'TX', '121', '200.00']]



### Step 5 - Combine lambda expressions with by-key arguments



```python
# Select two columns from text file

## Note: Need to create tuple (x,y)

pairs = clean.map(lambda lst: (lst[3],lst[-1]))
```


```python
# Display pairs

pairs.collect()
```




    [('State', 'Amount'),
     ('NY', '100.00'),
     ('TX', '450.00'),
     ('CA', '200.00'),
     ('CA', '500.00'),
     ('NY', '750.00'),
     ('TX', '200.00')]




```python
# Use reduce by key
# reduceByKey -> assumes first item is key and performs lambda function on second element (value)

rekey = pairs.reduceByKey(lambda amt1, amt2: float(amt1) + float(amt2))

```


```python
# Display the results

rekey.collect()
```




    [('State', 'Amount'), ('NY', 850.0), ('TX', 650.0), ('CA', 700.0)]




```python
# Review process
# step 1 - Grab States and Amounts in tuple form

step_1 = clean.map(lambda lst: (lst[3], lst[-1]))
```


```python
# Review process
# step 2 - Reduce by Key

step_2 = step_1.reduceByKey(lambda amt1, amt2: float(amt1) + float(amt2))
```


```python
# Review process
# step 3 - Get rid of headers (states and amounts)

step_3 = step_2.filter(lambda x: not x[0] == 'State')
```


```python
# Review process
# step 4 - Sort results by the amounts

step_4 = step_3.sortBy(lambda stAmount: stAmount[1], ascending=False)
```


```python
# Review process
# step 5 - Perform the Action 

step_4.collect()
```




    [('NY', 850.0), ('CA', 700.0), ('TX', 650.0)]



### Step 6 - Practice Tuple Unpacking



```python
x = ['ID', 'State', 'Amount']
```


```python
# Use tuple unpacking for readability

def func2(id_st_amt):
    # unpack the values
    (Id,st,amt) = id_st_amt
    return amt
    
```


```python
func2(x)
```




    'Amount'


