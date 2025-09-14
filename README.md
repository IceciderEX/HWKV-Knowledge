Extendible Hash Table 可扩展哈希详细理解:
https://zhuanlan.zhihu.com/p/622221722

设计目标是让哈希表的增长和收缩按需进行，从而避免传统静态哈希在表满时需要进行的“全表重哈希”（full-table rehashing）操作 。全表重哈希会暂停所有访问，导致系统吞吐量下降和查询延迟显著增加 。可扩展哈希通过增量操作解决了这个问题。

Directory：是存放bucket指针的容器，可动态生长（以原大小的倍数作为增长率），容器的每个元素可用哈希值来索引。
Bucket：桶。存放Key/value pair的桶，数据结构层面是一个线性表。

一个简单的可扩展哈希表：
![alt text](pics/image.png)

1. Global Depth：假设global depth为n，那么当前的directory必定有$2^n$个entry。同时，给定一个key，需要用global depth取出这个key的低n位的二进制值。例如，一个key的二进制是10111，如果global depth是3，通过IndexOf(key)函数，得到返回值的二进制值是111，即为7。这个值用来索引directory[111]位置的bucket。
2. Local Depth：local depth指的是（假设local depth为n）在当前的bucket之下，每个元素的key的低n位都是相同的。

特性：

B+树理解：
https://zhuanlan.zhihu.com/p/149287061

LSM-Tree Survey：
