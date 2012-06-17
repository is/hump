@Grab(group='org.codehaus.gpars', module='gpars', version='0.12')
@GrabConfig(systemClassLoader=true)
@Grab(group='mysql', module='mysql-connector-java', version='5.1.19')

import groovyx.gpars.GParsPool
// import groovyx.gpars.ParallelEnhancer
import groovy.sql.Sql
import groovy.transform.Field


@Field
def LogDBNameMap = ['rrwar': 'tr_log', 'rrlstx': 'sg2_log', ]

// --- Load configuration from properties file
@Field
Properties conf = new Properties()
new File(".scanner.properties").withInputStream {
	stream -> conf.load(stream)
}

@Field
def poolSize = conf.get("parallel", "60").toInteger()


def getDBEntriesList() {
	Sql sql = Sql.newInstance(conf["db.source.url"],
		conf["db.source.username"], conf["db.source.password"],
		conf["db.source.driver"])

	def rows = sql.rows "SELECT * FROM server_list"
	sql.close()
	return rows
}


def getTablesList(ds) {
	if (!LogDBNameMap.containsKey(ds.gameid))
		return null;

	String logDBName = LogDBNameMap[ds.gameid]
	String url = "jdbc:mysql://${ds.masterdb}:${ds.masterport}/information_schema";
	Sql sql = Sql.newInstance(url, ds.user, ds.pass)
	def rows = sql.rows (
		"SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_SCHEMA = :logdb",
		[logdb:logDBName])
	sql.close()
	return rows.collect { it ->
		it.dbname = logDBName
		it.ds = ds
		parseLogTableEntry(it)
		it
	}
}


def parseLogTableEntry(ts) {
	if (ts.name.contains('_log_')) {
		String[] tokens = ts.name.split('_log_', 2)
		ts.prefix = tokens[0]
		ts.postfix = tokens[1]
		ts.date = ts.postfix.replace('_', '')
		ts.isLog = true
	} else {
		ts.isLog = false
	}
}


def dbs = getDBEntriesList().findAll {it.gameid == 'rrwar'}
def res = null

GParsPool.withPool(poolSize) {
	dbs.makeConcurrent()
	res = dbs.collect { getTablesList(it) }
	dbs.makeSequential()
}

println res*.size()
println res[0][0]
