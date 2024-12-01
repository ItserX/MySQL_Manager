package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Table struct {
	name        string
	columns     []string
	columnTypes []*sql.ColumnType
}

func CloseCheck(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		log.Print(err.Error())
	}
}

func NewDBExplorer(db *sql.DB) (http.Handler, error) {
	tables := make(map[string]*Table)

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Print(err.Error())
		return nil, err
	}

	for rows.Next() {
		var tableName string

		err = rows.Scan(&tableName)
		if err != nil {
			log.Print(err.Error())
			return nil, err
		}

		tables[tableName] = &Table{name: tableName}
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	for _, table := range tables {
		rows, err = db.Query("SELECT * FROM " + table.name)
		if err != nil {
			log.Print(err.Error())
			return nil, err
		}
		rowsColumns, err := rows.Columns()
		if err != nil {
			log.Print(err.Error())
			return nil, err
		}
		tables[table.name].columns = rowsColumns

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			log.Print(err.Error())
			return nil, err
		}
		tables[table.name].columnTypes = columnTypes

		err = rows.Close()
		if err != nil {
			log.Print(err.Error())
			return nil, err
		}

	}

	reqs := []Req{
		{path: `^/{1}$`, method: "GET", function: GetTables(tables)},
		{path: `^\/[^\/?]+(:?(limit=\d+(&offset=\d+)?|offset=d+(&limit=\d+)?)?)?$`, method: "GET", function: GetTableValues(tables, db)},
		{path: `^/[a-zA-Z][a-zA-Z0-9]*/\d+$`, method: "GET", function: GetRecord(tables, db)},
		{path: `^/[a-zA-Z][a-zA-Z/0-9]*/$`, method: "PUT", function: PutRecord(tables, db)},
		{path: `^/[a-zA-Z][a-zA-Z/0-9]*/\d+$`, method: "POST", function: UpdateRecord(tables, db)},
		{path: `^/[a-zA-Z][a-zA-Z/0-9]*/\d+$`, method: "DELETE", function: DeleteRecord(tables, db)},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		method := r.Method
		f := PathDefinition(path, method, reqs)
		if f != nil {
			f(w, r)
		}
	})

	return mux, nil
}

type Req struct {
	path     string
	method   string
	function func(w http.ResponseWriter, r *http.Request)
}

var ErrTableNotFound = errors.New("unknown table")
var ErrBadRequest = errors.New("bad request")

func PathDefinition(path string, method string, reqs []Req) func(w http.ResponseWriter, r *http.Request) {
	for _, val := range reqs {
		reg, err := regexp.Compile(val.path)
		if err != nil {
			return nil
		}

		if reg.MatchString(path) && val.method == method {
			return val.function
		}
	}
	return nil
}

func CheckTable(w http.ResponseWriter, r *http.Request, tables map[string]*Table) (*Table, error) {
	path := strings.Split(r.URL.Path, "/")
	var tableName string
	if len(path) >= 2 {
		tableName = path[1]
	} else {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return nil, ErrBadRequest
	}

	table, ok := tables[tableName]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		resp := map[string]string{
			"error": "unknown table",
		}
		js, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil, err
		}
		_, err = w.Write(js)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil, err
		}
		return nil, ErrTableNotFound
	}
	return table, nil
}

var ErrCastingType = errors.New("casting type error")

func CastingValues(table *Table, values []interface{}) ([]interface{}, error) {
	for i := range values {

		if val, ok := values[i].(*interface{}); ok {
			values[i] = *val
		} else {
			return nil, ErrCastingType
		}

		var err error
		if values[i] != nil {

			val, ok := values[i].([]byte)
			if !ok {
				return nil, ErrCastingType
			}

			switch {
			case strings.Contains(table.columnTypes[i].ScanType().Name(), "int"):
				values[i], err = strconv.Atoi(string(val))
				if err != nil {
					return nil, err
				}
			case strings.Contains(table.columnTypes[i].ScanType().Name(), "float"):
				values[i], err = strconv.ParseFloat(string(val), 64)
				if err != nil {
					return nil, err
				}
			default:
				values[i] = string(val)
			}
		} else {
			values[i] = nil
		}
	}
	return values, nil
}

func GetTables(tables map[string]*Table) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		tableNames := make([]string, 0)
		for _, val := range tables {
			tableNames = append(tableNames, val.name)
		}
		sort.Slice(tableNames, func(i, j int) bool {
			return tableNames[i] < tableNames[j]
		})
		resp := map[string]map[string][]string{
			"response": {
				"tables": tableNames,
			},
		}
		js, err := json.Marshal(resp)
		if err != nil {
			return
		}
		fmt.Println(js)
		_, err = w.Write(js)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}
}

func GetTableValues(tables map[string]*Table, db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		table, err := CheckTable(w, r, tables)
		if err != nil {
			log.Print(err.Error())
			return
		}

		limQuery := r.URL.Query().Get("limit")
		offQuery := r.URL.Query().Get("offset")

		pattern := `^\d+$`
		limReg, err := regexp.MatchString(pattern, limQuery)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		offReg, err := regexp.MatchString(pattern, offQuery)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if limQuery == "" || !limReg {
			limQuery = "5"
		}

		if offQuery == "" || !offReg {
			offQuery = "0"
		}

		rows, err := db.Query("SELECT * FROM " + table.name + " LIMIT " + limQuery + " OFFSET " + offQuery)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer CloseCheck(rows)

		allValues := make([][]interface{}, 0)

		for rows.Next() {
			values := make([]interface{}, len(table.columns))
			for i := range values {
				values[i] = new(interface{})
			}

			err = rows.Scan(values...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			values, err = CastingValues(table, values)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			allValues = append(allValues, values)
		}

		resp := make([]map[string]interface{}, 0)
		for _, vals := range allValues {
			tmp := make(map[string]interface{})

			for i := 0; i < len(table.columnTypes); i++ {
				tmp[table.columnTypes[i].Name()] = vals[i]
			}
			resp = append(resp, tmp)
		}

		finalRes := map[string]interface{}{
			"response": map[string]interface{}{
				"records": resp,
			},
		}

		js, err := json.Marshal(finalRes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = w.Write(js)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}
}

func GetRecord(tables map[string]*Table, db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		table, err := CheckTable(w, r, tables)
		if err != nil {
			log.Print(err.Error())
			return
		}
		path := strings.Split(r.URL.Path, "/")

		idName := ""

		var id string
		if len(path) >= 3 {
			id = path[2]
		} else {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		for _, val := range table.columns {
			if strings.Contains(val, "id") {
				idName = val
			}
		}
		if idName == "" {
			http.Error(w, "unknown parameter", http.StatusBadRequest)
			return
		}

		var exists bool
		checkQuery := "SELECT EXISTS(SELECT * FROM " + table.name + " WHERE " + idName + " = ?" + ")"
		err = db.QueryRow(checkQuery, id).Scan(&exists)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !exists {
			resp := map[string]string{
				"error": "record not found",
			}
			js, errJs := json.Marshal(resp)
			if errJs != nil {
				http.Error(w, errJs.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, err = w.Write(js)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		values := make([]interface{}, len(table.columns))
		query := "SELECT * FROM " + table.name + " WHERE " + idName + " = " + id
		row, err := db.Query(query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer CloseCheck(row)

		for row.Next() {

			for i := range values {
				values[i] = new(interface{})
			}
			errRow := row.Scan(values...)
			if errRow != nil {
				http.Error(w, errRow.Error(), http.StatusInternalServerError)
				return
			}
		}

		values, err = CastingValues(table, values)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := make(map[string]interface{}, 0)

		for i := 0; i < len(table.columnTypes); i++ {
			resp[table.columnTypes[i].Name()] = values[i]
		}

		res := map[string]interface{}{
			"response": map[string]interface{}{
				"record": resp,
			},
		}

		js, err := json.Marshal(res)
		if err != nil {
			http.Error(w, "json marshal error", http.StatusInternalServerError)
			return
		}
		_, err = w.Write(js)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

}

func CheckColumn(val string, table *Table) bool {
	for _, columnName := range table.columns {
		if columnName == val {
			return true
		}
	}
	return false
}

func PutRecord(tables map[string]*Table, db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		table, err := CheckTable(w, r, tables)
		if err != nil {
			log.Print(err.Error())
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		values := make(map[string]interface{})
		for _, val := range table.columns {
			if CheckColumn(val, table) {
				values[val] = nil
			}
		}
		err = json.Unmarshal(body, &values)
		for key, _ := range values {
			if !CheckColumn(key, table) {
				delete(values, key)
			}
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		respFields := make([]string, 0)
		placeholders := make([]string, 0)
		respValues := make([]interface{}, 0)
		idName := ""
		for key, val := range values {
			switch {
			case !strings.Contains(key, "id") && CheckType(key, val, table):
				respFields = append(respFields, key)
				respValues = append(respValues, val)
				placeholders = append(placeholders, "?")
			case !strings.Contains(key, "id"):
				respFields = append(respFields, key)
				respValues = append(respValues, "")
				placeholders = append(placeholders, "?")
			default:
				idName = key
			}
		}

		fields := strings.Join(respFields, ", ")

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table.name, fields, strings.Join(placeholders, ", "))
		stmt, err := db.Prepare(query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer stmt.Close()

		res, err := stmt.Exec(respValues...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resID, err := res.LastInsertId()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{
			"response": map[string]interface{}{
				idName: resID,
			},
		}
		js, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = w.Write(js)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

	}

}

var FloatIntString = "float64 int32"

func CheckType(key string, currentVal interface{}, table *Table) bool {
	for _, column := range table.columnTypes {
		if key == column.Name() {
			nullable, _ := column.Nullable()
			if currentVal == nil && nullable {
				return true
			}
			if !nullable && currentVal == nil {
				return false
			}
			if reflect.TypeOf(currentVal).Name() == "string" && column.ScanType().Name() != "RawBytes" {
				return false
			}
			if !strings.Contains(FloatIntString, column.ScanType().Name()) && strings.Contains(FloatIntString, reflect.TypeOf(currentVal).Name()) {
				return false
			}
		}
	}
	return true
}
func UpdateRecord(tables map[string]*Table, db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		table, err := CheckTable(w, r, tables)
		if err != nil {
			log.Print(err.Error())
			return
		}
		path := strings.Split(r.URL.Path, "/")
		var id string
		if len(path) >= 3 {
			id = path[2]
		} else {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		idName := ""
		for i := range table.columns {
			if strings.Contains(strings.ToLower(table.columns[i]), "id") {
				idName = table.columns[i]
			}
		}
		var exists bool
		query := fmt.Sprintf("SELECT EXISTS (SELECT * FROM %s WHERE %s=?)", table.name, idName)
		err = db.QueryRow(query, id).Scan(&exists)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !exists {
			resp := map[string]string{
				"error": "record not found",
			}
			js, errJs := json.Marshal(resp)
			if errJs != nil {
				http.Error(w, errJs.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, err = w.Write(js)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		values := make(map[string]interface{})

		err = json.Unmarshal(body, &values)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fields := ""
		placeholders := make([]interface{}, 0)

		for key, val := range values {
			if !CheckType(key, val, table) || idName == key {
				resp := map[string]string{
					"error": "field " + key + " have invalid type",
				}
				js, errJs := json.Marshal(resp)
				if errJs != nil {
					http.Error(w, errJs.Error(), http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusBadRequest)
				_, err = w.Write(js)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}

			fields += "" + key + " = ?"
			placeholders = append(placeholders, val)

		}
		fields = strings.Replace(fields, "?", "?,", len(values)-1)
		query = fmt.Sprintf("UPDATE "+table.name+" SET %s WHERE %s = "+id, fields, idName)
		res, err := db.Exec(query, placeholders...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rowsAff, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{
			"response": map[string]interface{}{
				"updated": rowsAff,
			},
		}

		js, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = w.Write(js)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func DeleteRecord(tables map[string]*Table, db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		table, err := CheckTable(w, r, tables)
		if err != nil {
			return
		}
		path := strings.Split(r.URL.Path, "/")

		var id string
		if len(path) >= 3 {
			id = path[2]
		} else {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		idName := ""
		for i := range table.columns {
			if strings.Contains(strings.ToLower(table.columns[i]), "id") {
				idName = table.columns[i]
			}
		}

		query := "DELETE FROM " + table.name + " WHERE " + idName + " = ? "
		res, err := db.Exec(query, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rowsAff, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := map[string]interface{}{
			"response": map[string]interface{}{
				"deleted": rowsAff,
			},
		}
		js, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = w.Write(js)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
