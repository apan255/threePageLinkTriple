# IdentifingThreePageLinkTripleByMapReduce
To identify three-page link triples

[1]------->[2]----------->[3]

This support following parameters:
--------------------------------------------------------------------
1. Have simply generate the following output:
   <“3, 2”, 1 >
2. Webpages 3 and 2 are concatenated into a single value. If there are multiple webpages
having links to page 2, which further has links to page 3, the output should be:
<“3, 2”,“1, 4, 5, 6, 7”>
3. all pages in a three-page link triple need to be DISTINCT!
For example, triples corresponding to the following examples, i.e., <“3, 2”, 2 > or <“1, 1”, 1 >, 
should not be included in the output.
  __________________
  |                |
  |------>[1]<-----|          [2]<------------>[3]
  
  
  
  
  
