package main

import (
	"database/sql"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

var db *sql.DB

type Band struct {
	Name   string		`minLength:"4" maxLength:"30" json:"Name"`
	Year   int			`json:"Year"`
	Rating uint8		`minimum:"1" maximum:"4" default:"3" json:"Rating"`
}


func APIGetBands(c *gin.Context) {
	y := c.Query("year")
	r := c.Query("rating")

	if y!="" || r != "" {
		APIGetBandsByFilter(c)
		return
	}


	var Bands []Band

	rows, err := db.Query("select Name, Year, Rating from band")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"message":err.Error()})
		return
	}

	defer rows.Close()
	for rows.Next() {
		var band Band
		err := rows.Scan(&band.Name, &band.Year, &band.Rating)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"message":err.Error()})
			return
		}
		Bands = append(Bands, band)
	}

	err = rows.Err()
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"message":err.Error()})
		return
	}


	c.Header("Content-Type","application/json")
	c.JSON(http.StatusOK,Bands)

}

func APIGetBandByName(c *gin.Context) {
	var band Band
	bandname := c.Param("name")
	err := db.QueryRow("select Name, Year, Rating from band where Name = ?",bandname).Scan(&band.Name, &band.Year, &band.Rating)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message":err.Error()})
		return
	}
	c.Header("Content-Type","application/json")
	c.JSON(http.StatusOK,band)
}

func APIAddBand(c *gin.Context) {
	var band Band
	err := c.BindJSON(&band)
	if err != nil {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Error reading band data %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": s})
		return
	}

	// check for band record manually
	var dupCheck Band
	err = db.QueryRow("select Name, Year, Rating from band where Name = ?",band.Name).Scan(&dupCheck.Name,&dupCheck.Year,&dupCheck.Rating)

	// record exists
	if err == nil {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Band %s already exists in dataset", band.Name)
		c.JSON(http.StatusNotModified, gin.H{"message": s})
		return
	}


		stmt, err := db.Prepare("INSERT into band(Name,Year,Rating) VALUES(?,?,?)")
		if err != nil {
			log.Fatal(err)
		}

		res, err := stmt.Exec(band.Name,band.Year,band.Rating)
		if err != nil {
			log.Fatal(err)
		}

		rowCnt, err := res.RowsAffected()
		if err != nil {
			log.Fatal(err)
		}

		if rowCnt != 1 {
			log.Fatal(rowCnt)
		}

	s := fmt.Sprintf("http://localhost:8081/api/band/%s", band.Name)

	c.Header("Content-Type", "application/json")
	c.JSON(http.StatusCreated, gin.H{"message": s})

}

func APIGetBandsByFilter(c *gin.Context) {
	var year int
	c.Header("Content-Type","application/json")
	y := c.Query("year")
	year, _ = strconv.Atoi(y)

	var rating uint8
	r := c.Query("rating")
	r2, _ := strconv.Atoi(r)
	rating = uint8(r2)

	if year == 0 && rating == 0 {
		c.JSON(http.StatusBadRequest,gin.H{"message":"must filter by year or rating"})
		return
	}

	var Bands []Band
	var rows *sql.Rows
	var err error
	if year > 0 && rating == 0 {
		rows, err = db.Query("select Name, Year, Rating from band where Year = ?",year)
	} else
	if year == 0 && rating > 0 {
		rows, err = db.Query("select Name, Year, Rating from band where Rating = ?",rating)
	} else
	{
		rows, err = db.Query("select Name, Year, Rating from band where (Year = ? and Rating = ?)",year,rating)
	}

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"message":err.Error()})
		return
	}

	defer rows.Close()
	for rows.Next() {
		var band Band
		err := rows.Scan(&band.Name, &band.Year, &band.Rating)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"message":err.Error()})
			return
		}
		Bands = append(Bands, band)
	}

	err = rows.Err()
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"message":err.Error()})
		return
	}

	if len(Bands) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"message":"No matching records"})
		return
	}

	c.JSON(http.StatusOK,Bands)
}

func APIDeleteBand(c *gin.Context) {
	name := c.Param("name")
	stmt, err := db.Prepare("DELETE from Band where Name = ?")
	if err != nil {
		log.Fatal(err)
	}

	res, err := stmt.Exec(name)
	if err != nil {
		log.Fatal(err)
	}

	rowCnt, err := res.RowsAffected()
	if err != nil {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Band %s not found in dataset", name)
		c.JSON(http.StatusBadRequest, gin.H{"message": s})
		return
	}

	if rowCnt != 1 {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Band %s not found in dataset", name)
		c.JSON(http.StatusBadRequest, gin.H{"message": s})
		return
	}

	c.JSON(http.StatusNoContent, nil )

}

func APIPatchBandRating(c *gin.Context) {
	ratestr := c.Param("rate")
	name := c.Param("band")

	rate, err := strconv.Atoi(ratestr)

	if err != nil || name == "" || rate == 0 {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Bad Data Request or parameters")
		c.JSON(http.StatusBadRequest, gin.H{"message": s})
		return
	}

	// check for band record manually
	var dupCheck Band
	err = db.QueryRow("select NAME, YEAR, Rating from band where Name = ?",name).Scan(&dupCheck.Name,&dupCheck.Year, &dupCheck.Rating)

	// record exists
	if err != nil {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Band %s does not exist in dataset", name)
		c.JSON(http.StatusNotFound, gin.H{"message": s})
		return
	}


	stmt, err := db.Prepare("UPDATE band SET Rating=? WHERE Name =?")
	if err != nil {
		log.Fatal(err)
	}

	res, err := stmt.Exec(rate,name)
	if err != nil {
		log.Fatal(err)
	}

	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	if rowCnt != 1 {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Band %s not modified", name)
		c.JSON(http.StatusNotModified, gin.H{"message": s})
		return
	}

	s := fmt.Sprintf("http://localhost:8081/api/band/%s", name)

	c.Header("Content-Type", "application/json")
	c.JSON(http.StatusCreated, gin.H{"message": s})
}


func APIUpdateBand(c *gin.Context) {
	var band Band
	err := c.BindJSON(&band)
	if err != nil {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Error reading band data %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": s})
		return
	}

	// check for band record manually
	var dupCheck Band
	err = db.QueryRow("select Name, Year, Rating from band where Name = ?",band.Name).Scan(&dupCheck.Name,&dupCheck.Year,&dupCheck.Rating)

	// record exists
	if err != nil {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Band %s does not exist in dataset", band.Name)
		c.JSON(http.StatusNotFound, gin.H{"message": s})
		return
	}


	stmt, err := db.Prepare("UPDATE band SET Year=?,Rating=? WHERE Name =?")
	if err != nil {
		log.Fatal(err)
	}

	res, err := stmt.Exec(band.Year,band.Rating,band.Name)
	if err != nil {
		log.Fatal(err)
	}

	rowCnt, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	if rowCnt != 1 {
		c.Header("Content-Type", "application/json")
		s := fmt.Sprintf("Band %s not modified", band.Name)
		c.JSON(http.StatusNotModified, gin.H{"message": s})
		return
	}

	s := fmt.Sprintf("http://localhost:8081/api/band/%s", band.Name)

	c.Header("Content-Type", "application/json")
	c.JSON(http.StatusCreated, gin.H{"message": s})

}

func main() {
	defer db.Close()
	errChan := make(chan error)

	// notify if there is a Control-C or a Stop condition
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errChan <- fmt.Errorf("%s", <-c)
	}()

	router := gin.Default()

	router.GET("/api/band",APIGetBands)
	router.GET("/api/band/:name",APIGetBandByName)
	router.PATCH("/api/band/:band/:rate", APIPatchBandRating)
	router.PUT("/api/band", APIUpdateBand)
	router.POST("/api/band", APIAddBand)
	router.DELETE("/api/band/:name", APIDeleteBand)

	// query filter for year/rating is done via the APIGetBands

	router.Run(":8081")

}

func init() {
	var err error
	db, err = sql.Open("mysql","bandsys:p@ssword@tcp(172.17.1.125:3306)/bands")

	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

}
