package mree;

import mree.DBLoader.RandomText.text
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.system.measureTimeMillis


class DBLoader(createMissing: Boolean) {
    val strings = arrayOf("yfjietwnmmlihut", "sbaeidpdbxowrhx", "kmxpjsuoocyljob", "avcuwsopwubvjpd", "chbqygnjdtxyexn", "xxoywbsobxqndtj", "aocmgmxbeybjplv", "hrqmndjybhrywml", "mccssvqofcgpydm", "eyroretikbljdlo", "sghsglqvqcgkuqc", "suiuyydtsedglng", "fsippsyjilwlaxc", "bopbwsxrmlqajbf", "glubepqtqpiknxv", "mtgvuviyvltbybo", "nuddcradyumaxnk", "wyarugbabdpcopg", "cwfilcguyxsipjs", "jtipeyuvmumojhm", "ewndftiobnnnqga", "fmvwytmcrbeemre", "puopykmlkvevrtm", "smkybwktmftmmdq", "olhfygjbwixsctw", "odpktxhqwsomutd", "tolgwvrpmqpstvh", "vbkmitceyjpdopk", "strwrievcdsgonx", "wkpqhjmqxxakixn", "gkbxenajyjhvcmg", "fkcjkqplinsrdiy", "xcgdjcwhpljqicy", "haacchvkansacne", "pcmqjlvakrfxqiu", "bjibntniaavwwgf", "nqbdyorkfywrtkl", "bjreuijutqnemlb", "ffoyiwqkldgtqwj", "hagmmfsecierufm", "rtsmdxarcwcunch", "qmaaeqmxhpltcak", "inbjcohkkkdnvwo", "kkfcfccxgcipwfb", "xhkgbtiabsxyawl", "cxlxeupburupsvl", "xsdbflohmkwwjsq", "ofkcbqrtdoryuod", "tfufwsjcahaqmny", "wtacptlndgkbalu", "eokcmqaifpgegqw")
    val random = java.util.Random()

    init {
        if(createMissing){
            connectToDb()
        }else{
            connectToDb1()
        }
    }

    private fun connectToDb() {
        Database.connect("jdbc:h2:~/testdb;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver", user = "root", password = "")
        transaction {
            val create = measureTimeMillis { SchemaUtils.createMissingTablesAndColumns(RandomText) }
            println("h2 database creation #1 took $create ms")
            fillDb()
            println("Number of databases: "+ TransactionManager.current().db.dialect.allTablesNames().size)
        }
    }

    private fun connectToDb1() {
        Database.connect("jdbc:h2:~/testdb1;DB_CLOSE_DELAY=-1", driver = "org.h2.Driver", user = "root", password = "")
        transaction {
            val create = measureTimeMillis { SchemaUtils.create(RandomText) }
            println("h2 database creation #2 took $create ms")
            fillDb()
            println("Number of databases: "+ TransactionManager.current().db.dialect.allTablesNames().size)
        }
    }

    fun fillDb() {
        val fillingDb = measureTimeMillis {
            transaction {
                for(i in 0 until 50) {
                    RandomText.insert {
                        it[text] = generateRandomString()
                    }
                }
            }
        }
        println("Filling database with 50 random 15 long strings took $fillingDb ms")
        println("There's ${count()} text lines in the database!")
    }

    fun getData(): ArrayList<String> {
        val ret = ArrayList<String>()
        transaction {
            RandomText.selectAll().forEach {
                ret.add(it[text])
            }
        }
        return ret
    }

    fun count(): Int {
        return transaction {
            RandomText.selectAll().count()
        }
    }

    private fun generateRandomString(): String {
        return strings.random(random)
    }

    private fun Array<String>.random(random: java.util.Random): String = get(random.nextInt(size))

    object RandomText : Table() {
        val id = integer("id").autoIncrement()
        val text = varchar("text", length = 15)
        override val primaryKey = PrimaryKey(id)
    }
}
