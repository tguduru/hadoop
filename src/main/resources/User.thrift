namespace java org.hadoop.io.thrift.model

/**
* Details about a Person
*/
struct Person{
1: required i32 personId,
2: required string firstName,
3: optional string lastName,
4: optional string city
}