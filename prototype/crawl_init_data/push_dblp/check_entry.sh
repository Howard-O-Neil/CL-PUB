tail -n 1000000000 dblp.xml | grep "<article "
head -n 1000000000 dblp.xml | grep "<article "

tail -n 1000000000 dblp.xml | grep "<inproceedings "
head -n 1000000000 dblp.xml | grep "<inproceedings "

tail -n 1000000000 dblp.xml | grep "<proceedings "
head -n 1000000000 dblp.xml | grep "<proceedings "

tail -n 1000000000 dblp.xml | grep "<book "
head -n 1000000000 dblp.xml | grep "<book "

tail -n 1000000000 dblp.xml | grep "<incollection "
head -n 1000000000 dblp.xml | grep "<incollection "

tail -n 1000000000 dblp.xml | grep "<phdthesis "
head -n 1000000000 dblp.xml | grep "<phdthesis "

tail -n 1000000000 dblp.xml | grep "<mastersthesis "
head -n 1000000000 dblp.xml | grep "<mastersthesis "

tail -n 1000000000 dblp.xml | grep "<www "
head -n 1000000000 dblp.xml | grep "<www "

# Produce no results at all
tail -n 1000000000 dblp.xml | grep "<person "
head -n 1000000000 dblp.xml | grep "<person "

# Produce no results at all
tail -n 1000000000 dblp.xml | grep "<data "
head -n 1000000000 dblp.xml | grep "<data "
