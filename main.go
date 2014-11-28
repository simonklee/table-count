package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"text/tabwriter"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/simonz05/util/log"
)

var (
	help       = flag.Bool("h", false, "show help text")
	sqlDSN     = flag.String("sqlDSN", "root:@tcp(localhost:3306)/?utf8&parseTime=True", "MySQL Data Source Name")
	cpuprofile = flag.String("debug.cpuprofile", "", "write cpu profile to file")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <database> [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `
Description:
  finds diff between real COUNT(*) and AUTO_INCREMENT

  `)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	log.Println("Begin counting…")

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	db, err := sql.Open("mysql", *sqlDSN)

	if err != nil {
		log.Errorln(err)
		os.Exit(1)
	}

	sqlxDb := sqlx.NewDb(db, "mysql")
	defer db.Close()

	database := flag.Arg(0)

	if database == "" {
		flag.Usage()
		os.Exit(1)
	}

	tables, err := autoTables(sqlxDb, database)

	if err != nil {
		log.Fatal(err)
	}

	err = printTables(tables)

	if err != nil {
		log.Fatal(err)
	}
}

func autoTables(exec sqlx.Queryer, database string) ([]*table, error) {
	q := "SELECT TABLE_NAME, AUTO_INCREMENT"
	q += " FROM information_schema.TABLES"
	q += " WHERE TABLE_SCHEMA = ?"
	q += " AND AUTO_INCREMENT > 1 ORDER BY AUTO_INCREMENT DESC"

	rows, err := exec.Query(q, database)

	if err != nil {
		log.Errorf("find tables err: %v", err)
		return nil, err
	}

	defer rows.Close()
	tables := make([]*table, 0)

	for rows.Next() {
		table := new(table)
		err := rows.Scan(&table.name, &table.auto_increment)
		table.auto_increment--

		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	err = countTables(exec, database, tables)
	return tables, err
}

func countTables(exec sqlx.Queryer, database string, tables []*table) error {
	for _, table := range tables {
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", database, table.name)
		err := exec.QueryRowx(q).Scan(&table.count)

		if err != nil {
			log.Errorf("err: %s %v", q, err)
			return err
		}

	}

	return nil
}

type table struct {
	name           string
	auto_increment int
	count          int
}

func (t *table) String() string {
	return fmt.Sprintf("%s, AUTO_INCREMENT: %d, COUNT(*): %d", t.name, t.auto_increment, t.count)
}

func (t *table) diff() float64 {
	a, b := float64(t.auto_increment), float64(t.count)
	return (a - b) / ((a + b) / 2) * 100
}

func printTables(tables []*table) error {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 10, 0, 1, ' ', 0)

	fmt.Fprintln(w, "table\tAUTO\tcount\tΔ")

	for _, t := range tables {
		fmt.Fprintf(w, "%s\t%d\t%d\t%.2f%%\n", t.name, t.auto_increment, t.count, t.diff())
	}

	fmt.Fprintln(w)
	return w.Flush()
}
