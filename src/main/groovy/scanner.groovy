@Grab(group = 'org.codehaus.gpars', module = 'gpars', version = '0.12')
@GrabConfig(systemClassLoader = true)
@Grab(group = 'mysql', module = 'mysql-connector-java', version = '5.1.19') import groovy.json.JsonBuilder
import groovy.sql.Sql
import groovy.transform.Field
import groovyx.gpars.GParsPool

def getDBEntriesList() {
	Sql sql = Sql.newInstance(conf["db.source.url"],
		conf["db.source.username"], conf["db.source.password"],
		conf["db.source.driver"]) as Sql

	def rows = sql.rows "SELECT * FROM server_list"
	sql.close()
	return rows
}


List getMysqlTableList(Map ds, String dbname, Closure revise) {
	String url = "jdbc:mysql://${ds.masterdb}:${ds.masterport}/information_schema";
	Sql sql = Sql.newInstance(url, ds.user, ds.pass) as Sql

	String dateStr = conf.get('date')
	String query = null;

	if (dateStr == null) {
		query = "SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_SCHEMA = :dbname"
	} else if (dateStr.length() == 8) {
		dateStr2 = dateStr[0, 1, 2, 3] + "_" + dateStr[4, 5] + "_" + dateStr[6, 7]

		query = "SELECT TABLE_NAME as name FROM  information_schema.TABLES WHERE TABLE_SCHEMA = :dbname AND " +
			"(TABLE_NAME LIKE '%_${dateStr}' OR TABLE_NAME LIKE '%_${dateStr2}')"
	} else if (dateStr.length() == 6) {
		dateStr2 = dateStr[0, 1, 2, 3] + "_" + dateStr[4, 5]
		query = "SELECT TABLE_NAME as name FROM  information_schema.TABLES WHERE TABLE_SCHEMA = :dbname AND " +
			"(TABLE_NAME LIKE '%_${dateStr}%' OR TABLE_NAME LIKE '%_${dateStr2}_%')"
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


def getLogTablesList(ds) {
	if (!LogDBNameMap.containsKey(ds.gameid))
		return null;
	String logDBName = LogDBNameMap[ds.gameid]
	return getMysqlTableList(ds, logDBName) {ts ->
		if (ts.name.contains('_log_')) {
			String[] tokens = (ts.name as String).split('_log_', 2)
			ts.prefix = tokens[0]
			ts.postfix = tokens[1]
			ts.date = ts.postfix.replace('_', '')
			ts.isValid = true
		} else {
			ts.isValid = false
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
			host "${i.ds.masterdb}:${i.ds.masterport}"
			target "/z0/hump/log/${i.ds.gameid}/${i.prefix}/_${i.date}/${i.ds.sname}_${i.ds.sid}.rcfile"
			id "log.${i.ds.gameid}.${i.prefix}.${i.ds.sname}.${i.date}"
		}
		writer.write(json.toString())
		writer.write("\n")
	}
}


@Field
def LogDBNameMap = ['rrwar': 'tr_log', 'rrlstx': 'sg2_log',]

// --- Load configuration from properties file
@Field
Properties conf = new Properties()
new File(".scanner.properties").withInputStream {
	stream -> conf.load(stream)
}

@Field
int poolSize = conf.get("parallel", "60").toInteger()

CliBuilder cli = new CliBuilder(usage: 'scanner.groovy [options]', header: 'Options')
cli.o(longOpt: 'output', args: 1, argName: 'filename', 'Output files, default is hump-task.ajs')
cli.b(longOpt: 'begin', args: 1, argName: 'date', 'Begin of date range')
cli.e(longOpt: 'end', args: 1, argName: 'date', 'End of date range')
cli.d(longOpt: 'day', args: 1, argName: 'date', 'One day range')
cli.P(longOpt: 'parallel', args: 1, argName: 'num', 'Parallel task number')
cli.h(longOpt: 'help', 'Show usage information and quit')

OptionAccessor options = cli.parse(args)

if (options.h) {
	cli.usage()
	System.exit(0)
}

String outputFilename = 'hump-task.ajs'
if (options.o) {
	outputFilename = options.o
}
if (options.P) {
	poolSize = options.P.toInteger()
}

if (options.d) {
	conf['date'] = options.d
}


def dbs = getDBEntriesList()
def res = null

GParsPool.withPool(poolSize) {
	res = dbs.collectParallel { getLogTablesList(it) }.grep { it != null && it.size() > 0}
}


println res.size()
println res*.size()
println res.find {it != null && it.size != 0}[0]

new File(outputFilename).withWriter {writer ->
	res.each { it ->
		writeTableToLineJson(writer, it)
	}
}

// vim: ts=2 sts=2 ai