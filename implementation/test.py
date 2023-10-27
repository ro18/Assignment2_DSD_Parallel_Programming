"""
This is sample testing script, It is only for testing your code is working or not.
** It doesn't guarantee that your solution is 100% correct **

It tests that you have followed guidelines given in handout - * No global functions or variables *

"""
from q1.t1 import ThreadingSolution as Q1T1
from q1.t2 import MultiProcessingSolution as Q1T2
from q1.t3 import MPISolution as Q1T3

from q2.t1 import ThreadingSolution as Q2T1
from q2.t2 import MultiProcessingSolution as Q2T2
from q2.t3 import MPISolution as Q2T3

from q3.t1 import ThreadingSolution as Q3T1
from q3.t2 import MultiProcessingSolution as Q3T2
from q3.t3 import MPISolution as Q3T3

from q4.t1 import ThreadingSolution as Q4T1
from q4.t2 import MultiProcessingSolution as Q4T2
from q4.t3 import MPISolution as Q4T3

# Test Question 1
q1t1 = Q1T1(num_of_threads=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q1t1.run()
print(answer, timetaken)
q1t2 = Q1T2(num_of_processes=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q1t2.run()
print(answer, timetaken)
q1t3 = Q1T3(dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q1t3.run()
print(answer, timetaken)


# Test Question 2
q2t1 = Q2T1(num_of_threads=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q2t1.run()
print(answer, timetaken)
q2t2 = Q2T2(num_of_processes=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q2t2.run()
print(answer, timetaken)
q2t3 = Q2T3(dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q2t3.run()
print(answer, timetaken)


# Test Question 3
q3t1 = Q3T1(num_of_threads=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q3t1.run()
print(answer, timetaken)
q3t2 = Q3T2(num_of_processes=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q3t2.run()
print(answer, timetaken)
q3t3 = Q3T3(dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q3t3.run()
print(answer, timetaken)


# Test Question 4
q4t1 = Q4T1(num_of_threads=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q4t1.run()
print(answer, timetaken)
q4t2 = Q4T2(num_of_processes=4, dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q4t2.run()
print(answer, timetaken)
q4t3 = Q4T3(dataset_path="Combined_Flights_2021.csv", dataset_size=6311871)
answer, timetaken = q4t3.run()
print(answer, timetaken)
