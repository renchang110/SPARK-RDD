# RDD （弹性分布式数据集）

1. 转化，动作，持久化    对RDD每个操作都会产生独立的RDD，互不影响

2. 基本RDD转换操作   转换+执行

```python
intRDD = sc.parallelize([1,2,3,4,5])
intRDD.collect()
```

3. map 函数 运算     合并    转换算子 transtion

```python
# create->list  add   RDD no relation
intRDD = sc.parallelize([1,2,3,4,5])
intRDD.collect()
def addOne(x):
    return x+1
# int
1.
intRDD.map(addOne).collect()
intRDD.map(x => x+3).collect()
2.
intRDD.map(lambda x:x+2).collect()
结果：
[3, 4, 5, 6, 7]
# string
intRDD = sc.parallelize(['apple','banana','orange'])
intRDD.collect()
intRDD.map(lambda x : 'fruit:' + x).collect()
结果：
['fruit:apple', 'fruit:banana', 'fruit:orange']
```

4. filter 函数   过滤操作

```python
intRDD = sc.parallelize([1,2,3,4,5])
intRDD.collect()
# int
intRDD.filter(lambda x:x > 3 or x == 1).collect()
结果：
[1, 4, 5]
# string
intRDD = sc.parallelize(['apple','banana','orange'])
intRDD.collect()
# 查询所有带an的字符输出 list
intRDD.filter(lambda x : 'an' in x).collect()
结果：
['banana', 'orange']
```

5. distinct运算 删除重复的元素

```python
intRDD1 = sc.parallelize([1,1,2,2,3,4])
intRDD1.collect()
intRDD2 = sc.parallelize(['apple','apple','banana'])
intRDD2.collect()

print(intRDD.distinct().collect())
print(intRDD2.distinct().collect())
结果：
[2, 4, 1, 3]
['banana', 'apple']
```

6. randomSplit函数  可以将整个集合元素以随机数的方式按照比例分为多个RDD

```python
intRDD1 = sc.parallelize([1,1,2,2,3,4])
intRDD1.collect()

sRDD = intRDD1.randomSplit([0.4,0.6])
print(sRDD[0].collect())
sRDD[1].collect()
结果：
[3, 4]
[1, 1, 2, 2]
```

7. groupBy运算  按照传入的匿名函数规则将数据分为多个list

```python
intRDD = sc.parallelize([1,2,3,4,5])
intRDD.collect()

gRDD = intRDD.groupBy(
   lambda x : 'even' if (x%2 ==0) else 'odd'
).collect()

# 按照函数分为多个list 用sorted 查看值
print(gRDD[0][0],sorted(gRDD[0][1]))
print(gRDD[1][0],sorted(gRDD[1][1]))
结果：
even [2, 4]
odd [1, 3, 5]
```

## 1.多个RDD转换运算

1. 创建三个RDD样例

```python
intRDD1 = sc.parallelize([1,2,3,4])
intRDD2 = sc.parallelize([5,6])
intRDD3 = sc.parallelize(['apple','banana'])

print(intRDD1.collect())
print(intRDD2.collect())
print(intRDD3.collect())
结果：
[1, 2, 3, 4]
[5, 6]
['apple', 'banana']
```

2. union并集运算

```python
intRDD1.union(intRDD2).union(intRDD3).collect()
结果：
[1, 2, 3, 4, 5, 6, 'apple', 'banana']
```

3. 交集运算 intersection

```python
intRDD1 = sc.parallelize([1,2,3,4])
intRDD2 = sc.parallelize([4,5,6])

intRDD1.intersection(intRDD2).collect()
结果：
[4]
```

4. subtract 差集运算

```python
intRDD1 = sc.parallelize([1,2,3,4])
intRDD2 = sc.parallelize([4,5,6])

intRDD1.subtract(intRDD2).collect()
结果：
[1, 2, 3]
```

5. 笛卡尔积运算  cartesian()

```python
intRDD1 = sc.parallelize([1,2,3,4])
intRDD2 = sc.parallelize([4,5,6])
# （x,y）
print(intRDD1.cartesian(intRDD2).collect())
结果：
[(1, 4), (2, 4), (1, 5), (2, 5), (1, 6), (2, 6), (3, 4), (4, 4), (3, 5), (4, 5), (3, 6), (4, 6)]
```

##  2.基本动作运算

1. 读取元素

```python
intRDD = sc.parallelize(['banana','apple','pear'])
intRDD1 = sc.parallelize([1,2,3,4,5])

# 读取第一项
intRDD.first()
# 读取多项
print(intRDD.take(3))
# 按照从小到大的顺序读取前三项
intRDD.takeOrdered(3)

# 基于数字元素 按照从小到大的顺序读取前三项
print(intRDD1.takeOrdered(3))
# 基于数字元素 按照从大到小的顺序读取前三项
intRDD1.takeOrdered(3,key = lambda x : -x)
结果
banana
['banana', 'apple', 'pear']
['apple', 'banana', 'pear']
[1, 2, 3]
[5, 4, 3]
```

2. 统计功能

```python
intRDD1 = sc.parallelize([1,2,3,4,5])

# 统计 包含以下各项
print(intRDD1.stats())
# 最大值
print(intRDD1.max())
# 最小值
print(intRDD1.min())
# 标准差
print(intRDD1.stdev())
# 平均值
print(intRDD1.mean())
# 计个数
print(intRDD1.count())
# 总和
intRDD1.sum()
结果：
(count: 5, mean: 3.0, stdev: 1.4142135623730951, max: 5.0, min: 1.0)
5
1
1.4142135623730951
3.0
5
15
```

## 3.RDD key-value （键值对）基本转换运算

1. 创建范例RDD
2. filter 筛选key 运算
3. filter 筛选value 运算
4. mapValues 运算 （可以对RDD中每一对键值对进行运算，并且产生一个新的RDD）
5. sortByKey 从小到大按照key排序 （默认从小到大，ascending = False从大到小）
6. reduceByKey() 将拥有相同key 的value进行合并 

```python
# 创建范例RDD key-value
KVRDD = sc.parallelize([(1,2),(2,3),(3,4),(4,5)])

# 创建，输出key value值
print(KVRDD.collect())
print(KVRDD.keys().collect())
KVRDD.values().collect()
'''
[(1, 2), (2, 3), (3, 4), (4, 5)]
[1, 2, 3, 4]
[2, 3, 4, 5]
'''
# 筛选 key 以及 value 运算
KVRDD.filter(lambda keyvalue: keyvalue[0] <4).collect()
KVRDD.filter(lambda keyvalue: keyvalue[1] <4).collect()
'''
[(1, 2), (2, 3), (3, 4)]
[(1, 2), (2, 3)]
'''
# mapValues 运算 对value 进行平方
KVRDD.mapValues(lambda x : x*x).collect()
'''
[(1, 4), (2, 9), (3, 16), (4, 25)]
'''
# sortByKey 从小到大按照key排序，默认ascending = True ，false 从大到小
KVRDD.sortByKey(ascending = True).collect()
KVRDD.sortByKey().collect()
KVRDD.sortByKey(ascending = False).collect()
'''
[(1, 2), (2, 3), (3, 4), (4, 5)]
[(4, 5), (3, 4), (2, 3), (1, 2)]
'''
# reduceByKey() 将拥有相同key 的value进行合并
# KVRDD = sc.parallelize([(1,2),(2,3),(3,4),(4,5)])
KVRDD.reduceByKey(lambda x,y:x+y).collect()
'''
[(2, 3), (4, 7), (1, 2), (3, 4)]
'''
```

## 4.多个RDDkey-value （键值对）基本转换运算

1. 创建范例
2. join运算 将相同key的value连接起来
3. leftOuterJoin运算  左边的集合对应到右边，显示所有左边的元素，有相同key就join，没有就None
4. subtractByKey 运算 删除相同key的数据  

```python
a = sc.parallelize([(1,2),(2,3),(5,4)])
b = sc.parallelize([(5,6),(2,3)])

# 创建
a.collect()
b.collect()

# 连接 将相同key 的value 连接
a.join(b).collect()
'''
[(5, (4, 6)), (2, (3, 3))]
'''
# leftOuterJoin  and rightOuterJoin
a.leftOuterJoin(b).collect()
b.leftOuterJoin(a).collect()

a.rightOuterJoin(b).collect()
b.rightOuterJoin(a).collect()
'''
[(1, (2, None)), (5, (4, 6)), (2, (3, 3))]
[(5, (6, 4)), (2, (3, 3))]

[(5, (4, 6)), (2, (3, 3))]
[(5, (6, 4)), (1, (None, 2)), (2, (3, 3))]
'''
# subtractByKey 删除相同key的数据 不保留原数据
a.subtractByKey(b).collect()
'''
[(1, 2)]
'''
```

## 5. key-value动作运算

1. 取第一个数据
2. 取任意量的数据
3. 读取第一项数据的元素
4. 计算RDD中每一个key值的项数 countByKey
5. collectAsMap  创建key-value的字典  如果出现相同key 系统只会自动匹配其中一个
6. lookup 通过key查找value值
7. groupByKey   分组  在聚合
8. reduceByKey  聚类 在聚合

```python
a = sc.parallelize([(1,2),(2,3),(5,4)])
# 1，2
a.first()
a.take(2)
a.take(3)
'''
(1, 2)
[(1, 2), (2, 3)]
[(1, 2), (2, 3), (5, 4)]
'''
# 读取第一项数据的元素
c = a.first()
print(c)
print(c[0])
print(c[1])
'''
(1, 2)
1
2
''' 
# 计算RDD中每一个key值的项数  直接运行
a.countByKey()
'''
defaultdict(int, {1: 1, 2: 1, 5: 1})
'''
# 字典  collectAsMap  dict  赋值给d，通过key查询value
d = a.collectAsMap()
d[1]
'''
{1: 2, 2: 3, 5: 4}
'''
# lookup查询  通过key查询value
a.lookup(1)
'''
[2]
'''
```

## 6.Broadcast  广播变量

1. 共享变量，可以节省内存和运行时间  创建后不允许被修改
2. 对比

```python
# 不适用Broadcast 

a = sc.parallelize([(1,'apple'),(2,'banana'),(3,'watermelon'),(4,'orange')])
frult = a.collectAsMap()
b = sc.parallelize([1,3,2,4])
print('id:',b.collect())
print('dict:',a.collectAsMap())

print('id and dict change:')
# map函数 合并ab x是指b的值
c = b.map(lambda x : frult[x]).collect()
print('name:',str(c))
'''
结果
id: [1, 3, 2, 4]
dict: {1: 'apple', 2: 'banana', 3: 'watermelon', 4: 'orange'}
id and dict change:
name: ['apple', 'watermelon', 'banana', 'orange']
'''
# 使用broadcast  可以使用broadcast 类型的value函数直接匹配值（并行）
bc = sc.broadcast(frult)
print('broadcast change dic:')
d = b.map(lambda x : bc.value[x]).collect()
print('name:'+str(d))
'''
结果：
broadcast change dic:
name:['apple', 'watermelon', 'banana', 'orange']
'''

```

## 7.accumulator 累加器

1. 使用SparkContext.accumulator([初始值]) 来创建
2. 使用 .add()进行累加
3. 在task中，例如foreach 循环中，不能读取累加器的值
4. 只有驱动程序，也就是循环外，才可以使用 .value 来读取累加器的值 

```python
intrdd = sc.parallelize([2,1,3,4,5])
intrdd.collect()

# 创建两个累加器，double and int
total = sc.accumulator(0.0)
num = sc.accumulator(0)

# print(total)
# print(num)

# 利用foreach 传入参数，针对每一项数据执行    任务中无法进行读取value！
# foreach 没有返回值 （void）map 有返回值 返回一个新的流  action算子 
intrdd.foreach(lambda x : [total.add(x),num.add(x)])
# 操作后进行计数
avg = total.value/num.value
print(avg)
print(total.value)
print(num.value)
'''
结果：
3.0
15.0
5
'''
```

## 8.RDD persistence 持久化机制 

1. RDD持久化机制 是将需要重复运算的RDD存储在内存中，以便大幅提升运算效率

2. 使用：RDD.persist(存储等级)   ——可以指定存储等级，默认是MEMORY_ONLY 内存存储

3. 取消：RDD.unpersisit()  

4. 存储等级：1.默认MEMORY_ONLY 内存存储  反串行u化存储在JVM内存中  2. MEMORY_AND_DISK  内存存储不             够，存储到硬盘 3.MEMORY_ONLY_SER  串行化存储  用到是再进行反串行化 cpu资源占用多，节省内存 

   4.MEMORY_AND_DISK_SER  与上类似，多余的存储到硬盘  5.DISK_ONLY 存储到硬盘
   
5. spark的缓存机制  可以对数据设置缓存进行存储 之后可以直接用存储的数据

```python
rdd = sc.parallelize([1,2,3,4,5])
rdd.collect()

# 进行持久化
# 1.缓存方式1  底层调用也是MEMORY_ONLY
rdd.cache() 
# 2.缓存方式2
rdd.persist()
# 查看是否已经缓存
rdd.is_cached
# 结果为 True

# 取消持久化
rdd.unpersist()
rdd.is_cached
# 结果：False

# 添加存储等级
from pyspark import StorageLevel
rdd = sc.parallelize([1,2,3,4,5])
rdd.collect
# 可以通过设置 MEMORY_AND_DISK_1 MEMORY_AND_DISK_2 等设置相同RDD副本
rdd.persist(StorageLevel.MEMORY_AND_DISK)
rdd.is_cached
# 结果：True
```

## 9.wordcount  test

```python
# 创建一个测试文档  test.txt 
'''
Apple Apple orange
banana banana Grape
'''
# 读取内容
text = sc.textFile('data/test.txt')
# 按照空格切
testrdd = text.flatMap(lambda x : x.split(' '))
# testrdd.collect()
# 转化为元组并计数
testrdd2 = testrdd.map(lambda x : (x,1)).reduceByKey(lambda x,y : x + y)
# 储存 （存储到hdfs中，linux 本地存储失败）
# testrdd2.saveAsTextFile('file:data/Output')
testrdd2.saveAsTextFile('hdfs://master:9000/user/renchang/wordcount/data/Output')
# testrdd3 = testrdd2.collectAsMap()
# print(testrdd3)
```

