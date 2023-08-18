package main

import (
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

func TeslaMateAPICarsVampireDrainV1(c *gin.Context) {
	// define error messages
	var CarsVampireDrainError1 = "Unable to load vampire drain data."

	// getting CarID param from URL
	CarID := convertStringToInteger(c.Param("CarID"))
	// query options to modify query when collecting data
	ResultPage := convertStringToInteger(c.DefaultQuery("page", "1"))
	ResultShow := convertStringToInteger(c.DefaultQuery("show", "100"))

	// vampire-drain struct - child of Data
	type VampireDrain struct {
		DriveID     int     `json:"drive_id"`               // int
		StartDate   string  `json:"start_date"`             // string
		EndDate     string  `json:"end_date"`               // string
		Duration    int     `json:"duration"`               // int
		Period      int     `json:"period"`                 // int
		Standby     int     `json:"standby"`                // int
		SOC         int     `json:"soc_diff"`               // int
		Consumption float64 `json:"consumption"`            // float64
		AvgPower    float64 `json:"avg_power"`              // float64
		TRLossPer   int     `json:"range_lost_per_hour_km"` // int
		TRLoss      string  `json:"range_diff_km"`          // string
	}

	// Data struct - child of JSONData
	type Data struct {
		// Drives []Drives `json:"drives"` // Drives
		VampireDrain []VampireDrain `json:"vampire_drain"` // VampireDrain
	}

	// JSONData struct - main
	type JSONData struct {
		Data Data `json:"data"`
	}

	// creating required vars
	var (
		vampireDrains []VampireDrain
	)

	if ResultPage > 0 {
		ResultPage--
	} else {
		ResultPage = 0
	}
	ResultPage = (ResultPage * ResultShow)

	// query to collect data
	// getting data from database
	query := `
	with merge as (
		SELECT 
		   c.start_date AS start_date,
		   c.end_date AS end_date,
		   c.start_ideal_range_km AS start_ideal_range_km,
		   c.end_ideal_range_km AS end_ideal_range_km,
		   c.start_rated_range_km AS start_rated_range_km,
		   c.end_rated_range_km AS end_rated_range_km,
		   start_battery_level,
		   end_battery_level,
		   p.usable_battery_level AS start_usable_battery_level,
		   NULL AS end_usable_battery_level,
		   p.odometer AS start_km,
		   p.odometer AS end_km
		FROM charging_processes c
		JOIN positions p ON c.position_id = p.id
		WHERE c.car_id = $1 AND start_date BETWEEN '2023-05-20T08:22:34.273Z' AND '2023-08-18T08:22:34.273Z'
		UNION
		SELECT 
		   d.start_date AS start_date,
		   d.end_date AS end_date,
		   d.start_ideal_range_km AS start_ideal_range_km,
		   d.end_ideal_range_km AS end_ideal_range_km,
		   d.start_rated_range_km AS start_rated_range_km,
		   d.end_rated_range_km AS end_rated_range_km,
		   start_position.battery_level AS start_battery_level,
		   end_position.battery_level AS end_battery_level,
		   start_position.usable_battery_level AS start_usable_battery_level,
		   end_position.usable_battery_level AS end_usable_battery_level,
		   d.start_km AS start_km,
		   d.end_km AS end_km
		FROM drives d
		JOIN positions start_position ON d.start_position_id = start_position.id
		JOIN positions end_position ON d.end_position_id = end_position.id
		WHERE d.car_id = $1 
		ORDER BY start_date DESC
		LIMIT $2 OFFSET $3;
	   ), 
	   v as (
		SELECT
		   lag(t.end_date) OVER w AS start_date,
		   t.start_date AS end_date,
		   lag(t.end_rated_range_km) OVER w AS start_range,
		   t.start_rated_range_km AS end_range,
		   lag(t.end_km) OVER w AS start_km,
		   t.start_km AS end_km,
		   EXTRACT(EPOCH FROM age(t.start_date, lag(t.end_date) OVER w)) AS duration,
		   lag(t.end_battery_level) OVER w AS start_battery_level,
		   lag(t.end_usable_battery_level) OVER w AS start_usable_battery_level,
			   start_battery_level AS end_battery_level,
			   start_usable_battery_level AS end_usable_battery_level,
			   start_battery_level > COALESCE(start_usable_battery_level, start_battery_level) AS has_reduced_range
		 FROM merge t
		 WINDOW w AS (ORDER BY t.start_date ASC)
		 ORDER BY start_date DESC
	   )
	   
	   SELECT
		 round(extract(epoch FROM v.start_date)) * 1000 AS start_date_ts,
		 round(extract(epoch FROM v.end_date)) * 1000 AS end_date_ts,
		 -- Columns
		 v.start_date,
		 v.end_date,
		 v.duration,
		 (coalesce(s_asleep.sleep, 0) + coalesce(s_offline.sleep, 0)) / v.duration as standby,
		   -greatest(v.start_battery_level - v.end_battery_level, 0) as soc_diff,
		   CASE WHEN has_reduced_range THEN 1 ELSE 0 END as has_reduced_range,
		   convert_km(CASE WHEN has_reduced_range THEN NULL ELSE (v.start_range - v.end_range)::numeric END, 'km') AS range_diff_km,
		 CASE WHEN has_reduced_range THEN NULL ELSE (v.start_range - v.end_range) * c.efficiency END AS consumption,
		 CASE WHEN has_reduced_range THEN NULL ELSE ((v.start_range - v.end_range) * c.efficiency) / (v.duration / 3600) * 1000 END as avg_power,
		 convert_km(CASE WHEN has_reduced_range THEN NULL ELSE ((v.start_range - v.end_range) / (v.duration / 3600))::numeric END, 'km') AS range_lost_per_hour_km
	   FROM v,
		 LATERAL (
		   SELECT EXTRACT(EPOCH FROM sum(age(s.end_date, s.start_date))) as sleep
		   FROM states s
		   WHERE
			 state = 'asleep' AND
			 v.start_date <= s.start_date AND s.end_date <= v.end_date AND
			 s.car_id = $1
		 ) s_asleep,
		 LATERAL (
		   SELECT EXTRACT(EPOCH FROM sum(age(s.end_date, s.start_date))) as sleep
		   FROM states s
		   WHERE
			 state = 'offline' AND
			 v.start_date <= s.start_date AND s.end_date <= v.end_date AND
			 s.car_id = $1
		 ) s_offline
	   JOIN cars c ON c.id = $1
	   WHERE
		 v.duration > (6 * 60 * 60)
		 AND v.start_range - v.end_range >= 0
		 AND v.end_km - v.start_km < 1;`
	rows, err := db.Query(query, CarID, ResultShow, ResultPage)

	if err != nil {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsVampireDrainV1", CarsVampireDrainError1, err.Error())
		return
	}

	// collecting data
	for rows.Next() {
		var vampireDrain VampireDrain
		err := rows.Scan(
			&vampireDrain.StartDate,
			&vampireDrain.EndDate,
			&vampireDrain.Duration,
			&vampireDrain.Standby,
			&vampireDrain.SOC,
			&vampireDrain.TRLossPer,
			&vampireDrain.TRLoss,
			&vampireDrain.Consumption,
			&vampireDrain.AvgPower,
		)

		if err != nil {
			TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsVampireDrainV1", CarsVampireDrainError1, err.Error())
			return
		}
		vampireDrains = append(vampireDrains, vampireDrain)
	}

	// checking if data is empty
	if len(vampireDrains) == 0 {
		TeslaMateAPIHandleErrorResponse(c, "TeslaMateAPICarsVampireDrainV1", CarsVampireDrainError1, err.Error())
		return
	}

	// creating JSONData object
	jsonData := JSONData{
		Data: Data{
			VampireDrain: vampireDrains,
		},
	}

	// returning data
	TeslaMateAPIHandleSuccessResponse(c, "TeslaMateAPICarsVampireDrainV1", jsonData)
}
