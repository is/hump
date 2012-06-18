@Grab(group='org.codehaus.gpars', module='gpars', version='0.12')
@GrabConfig(systemClassLoader=true)
@Grab(group='mysql', module='mysql-connector-java', version='5.1.19')


import groovy.json.JsonBuilder
import groovy.sql.Sql
import groovy.transform.Field

import groovyx.gpars.GParsPool
// import groovyx.gpars.ParallelEnhancer

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


def writeTableToLineJson(Writer writer, entries) {
	if (entries == null)
		return
	if (entries.size() == 0)
		return


	JsonBuilder json = new JsonBuilder()
	entries.each { i ->
		if (!i.isLog)
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

def dbs = getDBEntriesList().findAll {it.gameid == 'rrwar'}
def res = null

GParsPool.withPool(poolSize) {
	res = dbs.collectParallel { getTablesList(it) }
}


println res.size()
println res*.size()
println res[0][0]

new File("hump-task.ajs").withWriter {writer ->
	res.each { it ->
		writeTableToLineJson(writer, it)
	}
}

// vim: ts=2 sts=2 ai
