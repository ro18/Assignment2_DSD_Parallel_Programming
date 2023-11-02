"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import math
import queue
from threading import Thread
import pandas as pd
import time
from tqdm import tqdm


class ThreadingSolution():
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_threads=None, dataset_path=None, dataset_size=None):
        self.num_of_threads = num_of_threads
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size
        self.result = None
       

    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """


        def reduce_task(mapping_output: dict):
            reduce_out = {}

            print("reduce")

            print(mapping_output)

            for out in tqdm(mapping_output):
                for key, value in out.items():

                    print(f"items:{out.items()}")

                    print("key dict")
                    print(f"key:{key}")


                    if key in reduce_out:
                        reduce_out[key] = reduce_out.get(key) + value
                    else:
                        reduce_out[key] = value
            

            print("max")
            print(max(reduce_out.values()))


            print("max key")


            print(max(reduce_out, key=reduce_out.get))


            return max(reduce_out, key=reduce_out.get)
        

        start_time = time.time()

        def map_tasks(reading_info: list, result_queue: queue):

    
            print(reading_info)



    
            df = pd.read_csv(self.dataset_path, nrows=reading_info[0], skiprows=reading_info[1], header=None)
            prefix = ['S','P']

            df['origin_with_S_P'] = df.iloc[:,2].str.startswith(tuple(prefix))

            # print("origin with S_P")

            # print(df['origin_with_S_P'])

            total_s_or_p = df['origin_with_S_P'].tolist().count(True)

            result = (
            df.groupby(df.iloc[:,1]).apply(lambda x : pd.Series({
                #'S_P_Percentage':  (x['origin_with_S_P'].sum() / total_s_or_p) * 100
                'S_P_Percentage':  x['origin_with_S_P'].sum()

            }))
            .reset_index()
            )


            # print("result from db")
            # print(result)

            #print(result.set_index(1)['S_P_Percentage'].to_dict())

            # self.final_result.append(result)

            # print(result)


            # if not self.final_result.empty:               
            #     self.final_result=pd.merge(result, self.final_result, how="outer")

            
            # #     self.final_result.append(result)


               
            # #     print("result self.final_result")
            # #     print(self.final_result)
            # else:
            #     self.final_result= result

            # print(result)
            # return_val[0] = result
            # print(result)

            result_queue.put(result.set_index(1)['S_P_Percentage'].to_dict())
            

            

    
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

            print(reading_info)
                       
            return reading_info
        



        chunk_distribution = distribute_rows()

       

        thread_handle = []


        # for x in range( 0,self.num_of_threads):
        #     return_val[x] = 0
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
            print("fin")
            result = result_queue.get()

            print(result)
            results.append(result)

            print("results")
            print(results)


       # df_final = self.final_result['S_P_Percentage'].div(self.num_of_threads, axis=1)


        print("final result")

        #print(self.final_result)

        # df_final = self.final_result.groupby(1).sum()   
        # df_final = df_final.reset_index()
        # print(df_final)

        # final_dict = self.final_result.set_index(1)['S_P_Percentage'].to_dict()



        #print(max(final_dict, key=final_dict.get))

        final_result = reduce_task(results)

        print(final_result)


        end_time = round(time.time() - start_time, 2)
   
 
                  


        #return max(final_dict, key=final_dict.get),end_time

        return (final_result,end_time)


if __name__ == '__main__':
    solution = ThreadingSolution(num_of_threads=4, dataset_path="implementation\Combined_Flights_2021.csv", dataset_size=6311871)

    #solution = ThreadingSolution(num_of_threads=4, dataset_path="C:\\Users\\R_CORDEI\Desktop\\DSD-A2\\Assignment2_DSD_Parallel_Programming\\Test.csv",dataset_size=7)



    answer, timetaken = solution.run()
    print(answer, timetaken)

