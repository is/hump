@Grab(group='org.codehaus.gpars', module='gpars', version='0.12')

import groovyx.gpars.GParsPool
import groovyx.gpars.ParallelEnhancer
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


def getDBEntriesList() {
	Sql sql = Sql.newInstance(conf["db.source.url"],
		conf["db.source.username"], conf["db.source.password"],
		conf["db.source.driver"])

	def rows = sql.rows "SELECT * FROM server_list"
	sql.close()
	return rows
}


def getTablesList(db) {
	if (!LogDBNameMap.containsKey(db.gameid))
		return null;

	String logDBName = LogDBNameMap[db.gameid]
	String url = "jdbc:mysql://${db.masterdb}:${db.masterport}/information_schema";
	Sql sql = Sql.newInstance(url, db.user, db.pass)
	def rows = sql.rows (
		"SELECT TABLE_NAME as name FROM information_schema.TABLES WHERE TABLE_SCHEMA = :logdb",
		[logdb:logDBName])
	sql.close()
	return rows
}

def dbs = getDBEntriesList().findAll {it-> it.gameid == 'rrlstx'}
println dbs.size()

//def a = getDBEntriesList().findAll({it -> it.gameid == 'rrwar'})
//print getTablesList(a[0])
//def list = [1, 2, 3, 4, 5, 6, 7, 8, 9];
//ParallelEnhancer.enhanceInstance list
//println list.collectParallel {it * 2}