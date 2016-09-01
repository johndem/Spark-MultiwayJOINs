# Spark-MultiwayJOINs
Perform 4 JOIN operations utilizing Spark's distributed environment.

The four relations under process are of this nature: R(a,b,c,value), A(a,x), B(b,y), C(c,z).

The JOIN operation is performed in three ways:

1. Transform the relation into RDDs (key,value) and use three binary join operations.

2. Transform the data into tables and use an SQL query.

3. 4-way JOIN operation according to the paper: “Foto N. Afrati, Jeffrey D. Ullman: Optimizing Multiway Joins in a Map-Reduce Environment. IEEE Trans. Knowl. Data Eng. 23(9): 1282-1298 (2011)”.
