def show (ds, title):
    def takeAndPrint(time, rdd):
        print('-------------------------------------------')
        print("Time: %s" % time, ' | ',title)
        print('-------------------------------------------')
        if not rdd.isEmpty():
            for x in rdd.toLocalIterator():
                print (x)
    ds.foreachRDD(takeAndPrint)
