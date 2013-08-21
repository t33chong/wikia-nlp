from WikiaSolr.SearchEval import CsvResultSetComparerGroup
import sys, codecs

group = CsvResultSetComparerGroup(sys.argv[1])
group.runTests()

#print group.csvComparison()

newfilename = sys.argv[2]
newfile = codecs.open(newfilename, 'w', 'utf-8')
newfile.write(group.csvComparison())
newfile.close()
print newfilename
