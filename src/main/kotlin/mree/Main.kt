package mree

class Main {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val db1 = DBLoader(false)
            val db2 = DBLoader(true)
            val db3 = DBLoaderKotliQuery()
        }
    }
}