from pyspark import SparkContext, SparkConf

conf = (SparkConf()
        .setMaster("local")
        .setAppName("My app")
        .set("spark.executor.memory", "1g"))
sc = SparkContext(conf=conf)

#pretotal_fee, start_dest_distance, time_cost, area, iscreate
def filter_feats(line, col_a, col_b, col_label):
    ret = True
    line = line.split('\t')
    line = [line[col_a], line[col_b], line[col_label]]
    try:
        [float(i) for i in line]
    except:
        print "cannot convert to float"
        ret = False
        pass
    return ret

def cross_feats(line, col_a, col_b, col_label):
    line = line.split('\t')
    line = [line[col_a], line[col_b], line[col_label]]
    return line

def filter_line(line):
    line = line.split('\t')
    return len(line) == 44

def line_format(line):
    line = line.split('\t')
    label = float(line[0])
    feats = [s.split(':')[1] for s in line[1:]]
    feats = [float(i) for i in feats]
    return [label] + feats

def cross(rdd, col_a, col_b, col_label):
    #rdd_src = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/bubble_basic_combinefe/2017/05/*')
    rdd = rdd.filter(lambda line: filter_feats(line, col_a, col_b, col_label))
    print "get the needed feats"
    rdd = rdd.map(lambda line: cross_feats(line, col_a, col_b, col_label))
    rdd = rdd.map(lambda line: [int(float(line[0])), int(float(line[1])), float(line[2])])
    rdd = rdd.map(lambda line: ((line[0], line[1]), (line[2], 1.0)))
    rdd = rdd.reduceByKey(lambda val1, val2: (val1[0]+val2[0], val1[1]+val2[1]))
    rdd = rdd.map(lambda line: ((line[0][0], line[0][1]), line[1][0]/line[1][1]))
    return rdd

#rdd2 is the  ((feat1, feat2), ecr) table
def join(rdd1, rdd2, col_a, col_b):
    rdd1 = rdd1.map(lambda line: ((int(line[col_a]), int(line[col_b])), line))
    #add the paired ecr in the last
    def add_ecr(line):
        key, ecr = line[0], line[1]
        ecr[0].append(ecr[1])
        return (key, ecr[0])
    rdd1 = rdd1.leftOuterJoin(rdd2).filter(lambda line: line[1][1] is not None).map(add_ecr).map(lambda line: line[1])
    return rdd1

if __name__=="__main__":
    rdd = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/bubble_basic_combinefe/2017/05/15/*')
    rdd2 = cross(rdd, 12, 11, 6)
    rdd1 = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/xgb_v2/2017/06/11/part-0009*')
    rdd1 = rdd1.filter(filter_line).map(line_format)
    rdd = join(rdd1, rdd2, 1, 5)
    print rdd.take(10)
