"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import math
import queue
from threading import Thread
import time
import pandas as pd

from tqdm import tqdm



class ThreadingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_threads=None, dataset_path=None, dataset_size=None):
        self.num_of_threads = num_of_threads
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """



        
        def reduce_task(mapping_output: dict):
            reduce_out = {}

            print("reduce")


            for out in tqdm(mapping_output):
                for key, value in out.items():

                    # print(f"items:{out.items()}")

                    # print("key dict")
                    # print(f"key:{key}")


                    if key in reduce_out:

                        if not math.isnan(value):
                            reduce_out[key] = reduce_out.get(key) + value         
                    else:
                        reduce_out[key] = value
            

            #print("max")
           # print(max(reduce_out.values()))


           # print("max key")


            #print(max(reduce_out, key=reduce_out.get))
            
            # for key, value in reduce_out.items():
            #     #print(f"value:{value}")
            #     reduce_out[key]= value/self.num_of_threads
            #     #print(reduce_out[key])

            return max(reduce_out, key=reduce_out.get)
    

        start_time = time.time()

        def map_tasks(reading_info: list, result_queue: queue):

    
            # print(reading_info)



    
            df = pd.read_csv(self.dataset_path, nrows=reading_info[0], skiprows=reading_info[1])

           # print("df df df")
            #print(df)
            #and df.iloc[:2] =='ATL')

            df_ATL = df[(pd.to_datetime(df.iloc[:,0]).dt.month == 11)]

            df_ATL= df_ATL[df_ATL.iloc[:,2].str.contains('ATL')]

            #print("hey")
            print(df_ATL)


            def get_hour(number):
            
                if (not math.isnan(number)):
                    if(len(str(number))) == 4:
                        
                        return (str(number)[:2])
                    else:
                        return (str(number)[:1])
              
                
    



            df_ATL['Hour']= df_ATL.iloc[:,6].apply(get_hour)

            #print("hour")

            #print(df_ATL["Hour"])

            #print("hourly")

            hourly_avg_count  = df_ATL['Hour'].value_counts()

            #print(hourly_avg_count.sort_values())

            result_queue.put(hourly_avg_count.to_dict())


            # busiest_hour = hourly_avg_count.idxmax()

            # print(busiest_hour)
        

            

    
        def distribute_rows():
            reading_info = []
            chunk_size =  self.dataset_size // self.num_of_threads 
            skip_rows = 1
            for _ in range(self.num_of_threads):
                if _ != self.num_of_threads-1:
                    reading_info.append([chunk_size, skip_rows])
                else:
                    reading_info.append([self.dataset_size - skip_rows, skip_rows])
                skip_rows += chunk_size

            #print(reading_info)
                       
            return reading_info
        



        chunk_distribution = distribute_rows()

       

        thread_handle = []

        result_queue = queue.Queue()

        for j in range(0, self.num_of_threads ):
            print(f"chunk:{chunk_distribution[j]}")

            t = Thread(target=map_tasks,args=(chunk_distribution[j],result_queue))
            thread_handle.append(t)

        for t in thread_handle:
            t.start()


        results = []

        for j in range(0, self.num_of_threads):
            thread_handle[j].join()
            #print("fin")
            result = result_queue.get()

            #print(result)
            results.append(result)

           # print("results")
            print(results)




        #print("final result")
        final_result = reduce_task(results)

        #print(final_result)


        end_time = round(time.time() - start_time, 2)
   

        return (final_result,end_time)


if __name__ == '__main__':
    solution = ThreadingSolution(num_of_threads=4, dataset_path="implementation/Combined_Flights_2021.csv", dataset_size=6311871)

    #solution = ThreadingSolution(num_of_threads=4, dataset_path="Test2.csv", dataset_size=5)

    answer, timetaken = solution.run()
    print(answer, timetaken)

