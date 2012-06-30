@Grab(group = 'org.codehaus.gpars', module = 'gpars', version = '0.12')
@GrabConfig(systemClassLoader = true)
@Grab(group = 'mysql', module = 'mysql-connector-java', version = '5.1.19') import groovy.json.JsonBuilder
import groovy.sql.Sql
import groovy.transform.Field
import groovyx.gpars.GParsPool
import groovy.transform.TypeChecked

def getDBEntriesList() {
	Sql sql = Sql.newInstance(conf["db.source.url"],
		conf["db.source.username"], conf["db.source.password"],
		conf["db.source.driver"]) as Sql

	def rows = sql.rows "SELECT * FROM server_list"
	sql.close()
	return rows.collect { it ->
		it.host = it.masterdb
		it.port = it.masterport
		it
	}
}

@TypeChecked
List getMysqlTableList(Map ds, String dbname, Closure revise) {
	String url = "jdbc:mysql://${ds.host}:${ds.port}/information_schema";
	Sql sql = Sql.newInstance(url, ds.user, ds.pass) as Sql
	String dateStr = conf.get('date')
	String query = null;

	String baseQuery = "SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_SCHEMA = :dbname"

	if (conf["skip.empty.table"] != null) {
		baseQuery += "AND TABLE_ROWS = 0"
	}

	if (dateStr == null) {
		query = baseQuery;
	} else if (dateStr.length() == 8) {
		dateStr2 = dateStr[0, 1, 2, 3] + "_" + dateStr[4, 5] + "_" + dateStr[6, 7]
		query = baseQuery + " AND (TABLE_NAME LIKE '%_${dateStr}' OR TABLE_NAME LIKE '%_${dateStr2}')"
	} else if (dateStr.length() == 6) {
		dateStr2 = dateStr[0, 1, 2, 3] + "_" + dateStr[4, 5]
		query = baseQuery + " AND TABLE_NAME LIKE '%_${dateStr}%' OR TABLE_NAME LIKE '%_${dateStr2}_%')"
	}

	def rows = sql.rows(query, [dbname: dbname])
	sql.close()
	return rows.collect { it ->
		it.dbname = dbname
		it.ds = ds
		if (revise != null) {
			revise(it)
		}
		it
	}
}


def getLogTableList(ds) {
	if (!LogDBNameMap.containsKey(ds.gameid))
		return null;
	String logDBName = LogDBNameMap[ds.gameid]
	return getMysqlTableList(ds, logDBName) {it ->
		if (it.name.contains('_log_')) {
			String[] tokens = (it.name as String).split('_log_', 2)
			it.prefix = tokens[0]
			it.postfix = tokens[1]
			it.date = it.postfix.replace('_', '')
			it.target = "log/${it.ds.gameid}/${it.prefix}/${it.date}/${it.ds.sname}__${it.ds.sid}"
			it.id = "log.${it.ds.gameid}.${it.prefix}.${it.ds.sname}.${it.date}"
			it.vc = [['sid', 'string', it.ds.sname], ['dt', 'int', it.date]]
			it.isValid = true
		} else {
			it.isValid = false
		}
	}
}


def getZ0TableList() {
	def db = [
		host: conf['z0.host'], port: conf['z0.port'],
		user: conf['z0.user'], pass: conf['z0.pass'],
	]

	return getMysqlTableList(db, conf['z0.db']) { it ->
		String tablename = it['name'] as String
		if (tablename =~ /_\d{8}$/) {
			it.prefix = tablename[0..-10]
			it.date = tablename[-8..-1]
			it.target = "account/${it.prefix}/${it.date}"
			it.id = "account.main.${it.prefix}.z0.${it.date}"
			it.vc = [['ds', 'int', it.date]]
			it.isValid = true
		} else {
			it.isValid = false
		}
	}
}


def writeTableToLineJson(Writer writer, entries) {
	if (entries == null)
		return
	if (entries.size() == 0)
		return


	JsonBuilder json = new JsonBuilder()
	entries.each { i ->
		if (!i.isValid)
			return

		json {
			username i.ds.user
			password i.ds.pass
			type "mysql"
			host "${i.ds.host}:${i.ds.port}"
			db i.dbname
			table i.name
			target i.target
			id i.id
			if (i.containsKey('vc')) {
				vc i.vc
			}
		}

		writer.write(json.toString())
		writer.write("\n")
	}
}


@Field
def LogDBNameMap = [
	'rrwar': 'tr_log',
	'rrlstx': 'sg2_log',
	'rrkd': 'tr_log',
	'rrd': 'dmbj_log',

	't': 'mmo_log',
//	'l': 'lzr_log',
	'cq': 'gen_log',
	'szcsj': 'tr_log',
]

// --- Load configuration from properties file
@Field
Properties conf = new Properties()
new File(".scanner.properties").withInputStream {
	stream -> conf.load(stream)
}

@Field
int poolSize = conf.get("parallel", "60").toInteger()

CliBuilder cli = new CliBuilder(usage: 'scanner.groovy [options]', header: 'Options')
cli.m(longOpt: 'mode', args: 1, argName: 'mode', 'Set scanner runmode')
cli.o(longOpt: 'output', args: 1, argName: 'filename', 'Output files, default is hump-task.ajs')
cli.b(longOpt: 'begin', args: 1, argName: 'date', 'Begin of date range')
cli.e(longOpt: 'end', args: 1, argName: 'date', 'End of date range')
cli.d(longOpt: 'day', args: 1, argName: 'date', 'One day range')
cli.P(longOpt: 'parallel', args: 1, argName: 'num', 'Parallel task number')
cli.h(longOpt: 'help', 'Show usage information and quit')
cli.E(longOpt: 'skipemptytable', 'Skip empty tables');

cli.z0('Run in mode z0')

OptionAccessor options = cli.parse(args)

if (options.h) {
	cli.usage()
	System.exit(0)
}


String outputFilename = 'hump-tasks.ajs'
String runMode = "log"

if (options.z0) {
	runMode = 'z0'
}

if (options.E) {
	conf['skip.empty.table'] = true;
}

if (options.m) {
	runMode = options.m
}
if (options.o) {
	outputFilename = options.o
}
if (options.P) {
	poolSize = options.P.toInteger()
}

if (options.d) {
	conf['date'] = options.d
}


def res = null

if (runMode == 'log') {
	println "Running in SCAN LOG DB mode."
	def dbs = getDBEntriesList()
	GParsPool.withPool(poolSize) {
		res = dbs.collectParallel { getLogTableList(it) }.grep { it != null && it.size() > 0}
	}
} else if (runMode == 'z0') {
	println "Running in Z0 mode."
	res = [getZ0TableList(),]
}

println res.size()
println res*.size()
println res.find {it != null && it.size != 0}.find {it.isValid == true}

new File(outputFilename).withWriter {writer ->
	res.each { it ->
		writeTableToLineJson(writer, it)
	}
}

// vim: ts=2 sts=2 ai
