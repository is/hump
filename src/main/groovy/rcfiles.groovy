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

if (descriptor.format == 'rcfile') {
	println("STORED AS RCFILE")
} else if (descriptor.format == 'text') {
	println("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\1'")
	println("STORED AS TEXTFILE")
}
println("LOCATION '${location}';")