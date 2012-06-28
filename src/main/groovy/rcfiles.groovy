import groovy.json.JsonSlurper

String tablename = args[0]
String location = args[1]
String descriptionFile = args[2]

def descriptor = new JsonSlurper().parse(new FileReader(descriptionFile));

String columns = descriptor.columns as String;
String columnTypes = descriptor.columnTypes as String;

String[] columnArray = columns.split(",");
String[] columnTypeArray = columnTypes.split(":");

println """CREATE EXTERNAL TABLE ${tablename} ("""
print("${columnArray[0]} ${columnTypeArray[0]}")
for (int i = 1; i < columnArray.length; ++i) {
	println(",")
	print("${columnArray[i]} ${columnTypeArray[i]}")
}

if (descriptor.vc != null) {
	for (vc in descriptor.vc) {
		println(",")
		print("${vc[0]} ${vc[1]}")
	}
}
println(")")
println("STORED AS RCFILE")
println("LOCATION '${location}';")