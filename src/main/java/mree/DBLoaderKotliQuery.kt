package mree

import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import org.jetbrains.exposed.sql.transactions.transaction
import kotlin.system.measureTimeMillis

class DBLoaderKotliQuery {
    lateinit var sess: Session
    val strings = arrayOf("yfjietwnmmlihut", "sbaeidpdbxowrhx", "kmxpjsuoocyljob", "avcuwsopwubvjpd", "chbqygnjdtxyexn", "xxoywbsobxqndtj", "aocmgmxbeybjplv", "hrqmndjybhrywml", "mccssvqofcgpydm", "eyroretikbljdlo", "sghsglqvqcgkuqc", "suiuyydtsedglng", "fsippsyjilwlaxc", "bopbwsxrmlqajbf", "glubepqtqpiknxv", "mtgvuviyvltbybo", "nuddcradyumaxnk", "wyarugbabdpcopg", "cwfilcguyxsipjs", "jtipeyuvmumojhm", "ewndftiobnnnqga", "fmvwytmcrbeemre", "puopykmlkvevrtm", "smkybwktmftmmdq", "olhfygjbwixsctw", "odpktxhqwsomutd", "tolgwvrpmqpstvh", "vbkmitceyjpdopk", "strwrievcdsgonx", "wkpqhjmqxxakixn", "gkbxenajyjhvcmg", "fkcjkqplinsrdiy", "xcgdjcwhpljqicy", "haacchvkansacne", "pcmqjlvakrfxqiu", "bjibntniaavwwgf", "nqbdyorkfywrtkl", "bjreuijutqnemlb", "ffoyiwqkldgtqwj", "hagmmfsecierufm", "rtsmdxarcwcunch", "qmaaeqmxhpltcak", "inbjcohkkkdnvwo", "kkfcfccxgcipwfb", "xhkgbtiabsxyawl", "cxlxeupburupsvl", "xsdbflohmkwwjsq", "ofkcbqrtdoryuod", "tfufwsjcahaqmny", "wtacptlndgkbalu", "eokcmqaifpgegqw")
    val random = java.util.Random()

    init {
        connectToDb()
    }

    private fun connectToDb() {
        val create = measureTimeMillis {
            sess = sessionOf("jdbc:h2:~/testdb;DB_CLOSE_DELAY=-1", "root", "")
            sess.execute(queryOf("CREATE TABLE IF NOT EXISTS RandomText(ID INT PRIMARY KEY auto_increment, text VARCHAR(15));"))
        }
        println("h2 database creation #3 took $create ms")
        sess.transaction {
            val fillingDb = measureTimeMillis {
                fillDb()
            }
            println("Filling database with strings took $fillingDb ms")
        }
    }

    fun fillDb() {
        val fillingDb = measureTimeMillis {
            sess.transaction {
                for (i in 0 until 50) {
                    sess.execute(queryOf("insert into RandomText (text) VALUES (?)", generateRandomString()))
                }
            }
        }
        println("Filling database with 50 random 5 long strings took $fillingDb ms")
    }

    fun getData(): ArrayList<String> {
        val ret = ArrayList<String>()
        transaction {
            val query = queryOf("select * from RandomText").map { row -> row.string("text") }.asList
            val list = sess.run(query)
            ret.addAll(list)
        }
        return ret
    }

    private fun getText() {

    }

    private fun generateRandomString(): String {
        return strings.random(random)
    }

    private fun Array<String>.random(random: java.util.Random): String = get(random.nextInt(size))
}