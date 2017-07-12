from pyspark import SparkContext, SparkConf

conf = (SparkConf()
        .setAppName("My app"))
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
    return len(line) == 40

def line_format(line):
    line = line.split('\t')
    label = float(line[0])
    feats = [s.split(':')[1] for s in line[1:]]
    feats = [float(i) for i in feats]
    return [label] + feats

def line_format_1(line):
    line = [str(i) for i in line]
    label, feat = line[0], line[1:]
    feat = [str(ind+1)+':'+feat[ind] for ind in range(len(feat))]
    feat_str = "\t".join(feat)
    feat_str = label+"\t"+feat_str
    return feat_str

def filter_by_float(line):
    ret = True
    try:
        [float(i) for i in line]
    except:
        ret = False
    return ret

##format is libsvm
def cross(rdd, col_a, col_b, col_label):
    rdd = rdd.filter(filter_line)
    rdd = rdd.map(line_format)
    rdd = rdd.map(lambda line: [line[ind] for ind in [col_a, col_b, col_label]])
    rdd = rdd.filter(filter_by_float)
    rdd = rdd.map(lambda line: [int(float(line[0])), int(float(line[1])), float(line[2])])
    rdd = rdd.map(lambda line: ((line[0], line[1]), (line[2], 1.0)))
    rdd = rdd.reduceByKey(lambda val1, val2: (val1[0]+val2[0], val1[1]+val2[1]))
    rdd = rdd.map(lambda line: ((line[0][0], line[0][1]), line[1][0]/line[1][1]))
    return rdd

def filter_feats_lis(line, lis, col_label):
    ret = True
    line = [line[ind] for ind in lis+[col_label]]
    try:
        [float(i) for i in line]
    except:
        print "cannot covert to float"
        ret = False
        pass
    return ret

def cross_feats_lis(line, lis, col_label):
    line = line.split('\t')
    line = [line[ind] for ind in lis+[col_label]]
    return line

def cross_lis(rdd, col_lis, col_label):
    rdd = rdd.filter(lambda line: filter_feats_lis(line, col_lis, col_label))
    rdd = rdd.map(lambda line: cross_feats_lis(line, col_lis, col_label))
    rdd = rdd.map(lambda line: [tuple([int(float(line[ind])) for ind in range(len(col_lis))]), float(line[-1])])
    rdd = rdd.map(lambda line: (line[0], (line[1], 1.0)))
    rdd = rdd.reduceByKey(lambda val1, val2: (val1[0]+val2[0], val1[0]+val2[0]))
    rdd = rdd.map(lambda line: (line[0], line[1][0]/line[1][1]))
    return rdd

#rdd2 is the  ((feat1, feat2), ecr) table
#rdd1 is format line list
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
    #index start from 0
    ##do something on statistics set
    area_col, time_cost_col, start_dest_distance_col, nocarpool_fee_col, iscreate_col  = 2, 3, 6, 5, 0
    rdd = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/xgb_v2_byday/2017/05/*')
    #area_time_ecr_table = cross(rdd, area_col, time_cost_col, iscreate_col)
    #area_dis_ecr_table = cross(rdd, area_col, start_dest_distance_col, iscreate_col)
    #area_fee_ecr_table = cross(rdd, area_col, nocarpool_fee_col, iscreate_col)
    nocarpoolfee_dis_ecr_table = cross(rdd, nocarpool_fee_col, start_dest_distance_col, iscreate_col)
    nocarpoolfee_dis_ecr_table.coalesce(10).saveAsTextFile('/user/bigdata_driver_ecosys_test/xiajizhong/nocarpoolfee_dis_ecr_table')
    nocarpoolfee_time_ecr_table = cross(rdd, nocarpool_fee_col, time_cost_col, iscreate_col)
    nocarpoolfee_time_ecr_table.coalesce(10).saveAsTextFile('/user/bigdata_driver_ecosys_test/xiajizhong/nocarpoolfee_time_ecr_table')
    #area_fee_ecr_table.coalesce(30).saveAsTextFile('/user/bigdata_driver_ecosys_test/xiajizhong/area_fee_ecr_table')

    ##do something on train set
    #rdd_lis = []
    #rdd = sc.textFile('/user/bigdata_driver_ecosys_test/cgb/price_fe/xgb_v2_byday/2017/06/0*')
    rdd = sc.textFile('/user/bigdata_driver_ecosys_test/xiajizhong/201706_val/*')
    rdd = rdd.map(line_format)
    """
    rdd = join(rdd, area_time_ecr_table, area_col, time_cost_col)
    rdd = join(rdd, area_dis_ecr_table, area_col, start_dest_distance_col)
    rdd = join(rdd, area_fee_ecr_table, area_col, nocarpool_fee_col)
    """
    rdd = join(rdd, nocarpoolfee_dis_ecr_table, nocarpool_fee_col, start_dest_distance_col)
    rdd = join(rdd, nocarpoolfee_time_ecr_table, nocarpool_fee_col, time_cost_col)
    rdd = rdd.map(line_format_1)
    #rdd.coalesce(300).saveAsTextFile('/user/bigdata_driver_ecosys_test/xiajizhong/201706_val')
    rdd.coalesce(300).saveAsTextFile('/user/bigdata_driver_ecosys_test/xiajizhong/201706_val_nocarpoolfee_cross')
