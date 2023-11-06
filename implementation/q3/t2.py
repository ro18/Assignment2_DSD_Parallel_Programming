"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import itertools
import math
from threading import Thread
import pandas as pd
import time
from tqdm import tqdm
from pathos.multiprocessing import Pool


class MultiProcessingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_processes=None, dataset_path=None, dataset_size=None):
        self.num_of_processes = num_of_processes
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """

        
        start = time.time()


        def reduce_task(mapping_output: dict):
            reduce_out = {}

            none_values = sum(x is None for x in mapping_output)

            print(none_values)




            mapping_output = [x for x in mapping_output if x is not None]

            print(mapping_output)



            for out in tqdm(mapping_output):
                for key, value in out.items():

                    print(f"items:{out.items()}")

                    print("key dict")
                    print(f"key:{key}")


                    if key in reduce_out:
                        if not math.isnan(value):
                            reduce_out[key] = reduce_out.get(key) + value
                    else:
                        reduce_out[key] = value

                    print("output now")
                    print(reduce_out)
            

               
           


            print("max")

            # print(reduce_out)
            print(max(reduce_out.values()))

            for key, value in reduce_out.items():
                print(f"value:{value}")
                reduce_out[key]= value / (self.num_of_processes - none_values)
                print(reduce_out[key])

            print("max key")


            print(max(reduce_out, key=reduce_out.get))

            print("max element value")
            print(reduce_out.get(max(reduce_out, key=reduce_out.get)))



            return max(reduce_out, key=reduce_out.get)
        
        def distribute_rows(n_rows: int, n_processes):
            reading_info = []
            chunk_size =  n_rows // n_processes
            skip_rows = 1
            for _ in range(n_processes):
                if _ != n_processes-1:
                    reading_info.append([chunk_size, skip_rows])
                else:
                    reading_info.append([self.dataset_size - skip_rows, skip_rows])
                skip_rows += chunk_size

            # print(reading_info)
                        
            return reading_info
        
        
        def map_tasks(reading_info: list,data : str):

    
            # print(reading_info)



    
            df = pd.read_csv(self.dataset_path, nrows=reading_info[0], skiprows=reading_info[1], header=None)


            
            df['arr_delay_check'] = df.iloc[:,55] < 0

            # print(df.columns.tolist()) 

            df['Quarter'] = pd.PeriodIndex(df.iloc[:,0], freq='Q-DEC').strftime('Q%q')

            print(df['Quarter'])


            df2Quarter = df.loc[df['Quarter'] == "Q1"]

            print("df2Quarter")

            print(df2Quarter)

            # print("count")

            # print(df2Quarter['Quarter'].count())
            if not df2Quarter.empty:
                print("in here")
                result = (
                df2Quarter.groupby(df2Quarter.iloc[:,1]).apply(lambda x : pd.Series({
                    'arrive_early_per':  ( x['arr_delay_check'].sum() / df2Quarter['Quarter'].count() ) * 100

                }))
                .reset_index()
                )

                print("look for this")

                print(result)

                print(result.set_index(1)['arrive_early_per'].to_dict())






                return result.set_index(1)['arrive_early_per'].to_dict()


        p = Pool(processes=self.num_of_processes)
        result = p.starmap(map_tasks, zip(distribute_rows(n_rows=self.dataset_size, n_processes=self.num_of_processes),itertools.repeat(self.dataset_path)))

        print("after processing")

        print(result)


       





        final_result = reduce_task(result)
        p.close()
        p.join()

        return final_result, round(time.time()-start,2)



if __name__ == '__main__':
    #solution = MultiProcessingSolution(num_of_processes=4, dataset_path="Test2.csv", dataset_size=4)

    solution = MultiProcessingSolution(num_of_processes=4, dataset_path="implementation/Combined_Flights_2021.csv", dataset_size=6311871)
    result, timetaken = solution.run()
    print(result, timetaken)

