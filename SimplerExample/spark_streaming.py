
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def run_spark():
	""" 1.creates spark context
	2.creates streaming spark context 
	3.assign the port in emr where you get the stream (nc -lk 9999)
	4.define the map and reduce to do word count
	5.start the process
	"""


	sc = SparkContext("local[2]") # define for the master node
	sc.setLogLevel('ERROR') #only log errors

	# Create the streaming context from the SparkContext object
	ssc = StreamingContext(sc,2) # Latency 2 seconds

	## determine where your stream is coming from
	sentence = ssc.socketTextStream("localhost", 9999)
	words = sentence.flatMap(lambda sent: sent.split(" "))

	# Count each word in each batch
	key_val = words.map(lambda word: (word, 1))
	wordCounts = pairs.reduceByKey(lambda x, y: x + y)

	# Print the first ten elements of each RDD generated in this DStream to the console
	wordCounts.pprint()

	#starts the streaming process
	ssc.start()
	ssc.awaitTermination()

if __name__ =="__main__":
	run_spark()