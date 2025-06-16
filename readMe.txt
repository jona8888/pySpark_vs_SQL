
In this project, a large text dataset, 
derived from Modern Operating Systems by Andrew Tanenbaum(ModernOperatingSystems.docx), 
is analyzed with the PySpark API and PySpark with SQL. These tasks build off of 
homework 3 and includes total word count, most frequent words, and word pair frequency.

Before running make sure that ModernOperatingSystems.docx is in the same directory as:
finalProj.py, milestone100Stop.py or milestone100GO.py, depending on which one you want to run!

----------------------------------------------

To run the main final project code:

	python3 finalProj.py


This will run all three tasks(total word count, most frequent words, and word pair frequency) 
using both API and SQL, it will then output performance comparisons and write CSV results.
----------------------------------------------




Tester File 1:

	 python3 milestone100Stop.py


This code runs Word Count and Top 20 frequent words. 
This code also runs Word Pairs(task 3) 100 times and stops when the API has a faster time for Word Pairs. 
This demonstrates that although the SQL method outperforms the API version a majority of the time, 
sometimes the API version does record a faster time.  



SQL consistently runs faster than the API on the word pair task. for example:

 a single outlier occurred on 
run 41 (API: 4.1118s vs SQL: 4.1643s), highlighting how execution environment can occasionally 
shift performance dynamics — despite identical logic.”


For example:

"API wins on run 41!
API: 4.1118 sec | SQL: 4.1643 sec"

------------------------------------------------------------




Test File 2:

	python3 milestone100Go.py  

I then realized that I don't want the code to stop just because the API returns a faster runtime for word pairs. 

It returns a count of how many times the API was faster than SQL after 100 runs of Word Pairs(task 3). 
So I can get a clear percentage! I did not change the original code 
or logic at all and I did not reduce the dataset to favor PySpark API

(This code runs Word Count and Top 20 frequent words 
followed by Word Pairs 100 times; returning the number of times that the API runs faster than the SQL method, after the 100th run)

for example: "API was faster than SQL in 2 out of 100 runs."


