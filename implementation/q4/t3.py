"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import time
import math
import pandas as pd
from mpi4py import MPI
import pandas as pd
import time
from tqdm import tqdm

class MPISolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self):
        """
        Returns the tuple of computed result and time taken. eg., ("I am final Result", 3.455)
        """

        
        def reduce_task(mapping_output: dict, size:int):
            reduce_out = {}

            none_values = sum(x is None for x in mapping_output)

            mapping_output = [x for x in mapping_output if x is not None]




            for out in tqdm(mapping_output):
                for key, value in out.items():

                   
                    if key in reduce_out:
                        if not math.isnan(value):
                            reduce_out[key] = reduce_out.get(key) + value
                    else:
                        reduce_out[key] = value

               
           


            #print("max")

            # print(reduce_out)
            #print(max(reduce_out.values()))

            # for key, value in reduce_out.items():
            #     #print(f"value:{value}")
            #     reduce_out[key]= value / (size - none_values - 1)
            #    # print(reduce_out[key])

            #print("max key")


           # print(max(reduce_out, key=reduce_out.get))

            #print("max element value")
            #print(reduce_out.get(max(reduce_out, key=reduce_out.get)))



            return max(reduce_out, key=reduce_out.get)
        
        start = time.time()
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()


        results = []

        if rank == 0:

            #rint("in rank 0")

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

            slave_workers = size - 1
            chunk_distribution = distribute_rows(n_rows=self.dataset_size, n_processes=slave_workers)

            # distribute tasks to slaves
            
            for worker in range(1, size):
                chunk_to_process = worker-1
                #print(f"worker:{worker}")
                comm.send(chunk_distribution[chunk_to_process], dest=worker)

        # receive and aggregate results from slave

            for worker in (range(1, size)):
                #print("workers")  # receive
                result = comm.recv(source=worker)
                #print(result)
                results.append(result)


            
            print("results")

            print(results)
        



            
        
            final_results = reduce_task(results,comm.Get_size())

            print("final_results")

            print(final_results)

            return final_results, round(time.time() - start,2)
        




        elif rank > 0:
            chunk_to_process = comm.recv()
           # print(chunk_to_process)
            df = pd.read_csv(self.dataset_path, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)


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

            comm.send(hourly_avg_count.to_dict(), dest=0)




            return ("none from slaves","NA")


if __name__ == '__main__':
    solution = MPISolution(dataset_path="implementation/Combined_Flights_2021.csv", dataset_size=6311871)
    answer, timetaken = solution.run()
    print(answer, timetaken)

