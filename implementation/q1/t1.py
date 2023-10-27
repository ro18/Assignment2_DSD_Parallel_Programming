"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import threading
import pandas as pd
import time


class ThreadingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_threads=None, dataset_path=None, dataset_size=None):
        self.num_of_threads = num_of_threads
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def getCount():
        print("get count")


    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """
        df = pd.read_csv(self.dataset_path)
        
        #print(f"no of rows:{len(df)}")

        #print(df.iloc[:,1])
        starts =['S','P']
        matched_rows=df.iloc[:,4].str.startswith('S')
        print(df[matched_rows])
        data = pd.DataFrame(matched_rows,columns=['Row','Value'])
       
  
            


        return ("4","5")


if __name__ == '__main__':
    solution = ThreadingSolution(num_of_threads=4, dataset_path="implementation\Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

