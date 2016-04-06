package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
)

func printTableSchema() {
	createTable := `CREATE TABLE detections
(
  pkid serial,
  id integer,
  id_key text,
  date_proc timestamp without time zone,
  lat_gmtco double precision,
  lon_gmtco double precision,
  date_mscan timestamp without time zone,
  temp_bb double precision,
  temp_bkg double precision,
  esf_bb double precision,
  rhi double precision,
  rh double precision,
  methane_eq double precision,
  co2_eq double precision,
  area_pixel double precision,
  area_bb double precision,
  cloud_mask integer,
  qf_fit integer,
  qf_detect integer,
  rad_dnb double precision,
  rad_m07 double precision,
  rad_m08 double precision,
  rad_m10 double precision,
  rad_m12 double precision,
  rad_m13 double precision,
  rad_m14 double precision,
  rad_m15 double precision,
  rad_m16 double precision,
  tran_dnb double precision,
  tran_m07 double precision,
  tran_m08 double precision,
  tran_m10 double precision,
  tran_m12 double precision,
  tran_m13 double precision,
  tran_m14 double precision,
  tran_m15 double precision,
  tran_m16 double precision,
  pthm_m12 double precision,
  pthm_m13 double precision,
  pthm_m14 double precision,
  pthm_m15 double precision,
  pthm_m16 double precision,
  qf1_dnb integer,
  qf1_m07 integer,
  qf1_m08 integer,
  qf1_m10 integer,
  qf1_m12 integer,
  qf1_m13 integer,
  qf1_m14 integer,
  qf1_m15 integer,
  qf1_m16 integer,
  cot_ivcop double precision,
  eps_ivcop double precision,
  qf1_ivcop integer,
  qf2_ivcop integer,
  qf3_ivcop integer,
  dn_m10 integer,
  sample_m10 integer,
  line_m10 integer,
  sample_bt integer,
  line_bt integer,
  sample_dnb integer,
  line_dnb integer,
  lat_dnb double precision,
  lon_dnb double precision,
  dist_dnb double precision,
  thr_m07 double precision,
  thr_m08 double precision,
  thr_m10 double precision,
  thr_m16 double precision,
  solz_gmtco double precision,
  sola_gmtco double precision,
  satz_gmtco double precision,
  sata_gmtco double precision,
  scvx_gmtco double precision,
  scvy_gmtco double precision,
  scvz_gmtco double precision,
  scpx_gmtco double precision,
  scpy_gmtco double precision,
  scpz_gmtco double precision,
  scax_gmtco double precision,
  scay_gmtco double precision,
  scaz_gmtco double precision,
  qf1_gmtco integer,
  qf2_gmtco integer,
  qf1_iicmo integer,
  qf2_iicmo integer,
  qf3_iicmo integer,
  qf4_iicmo integer,
  qf5_iicmo integer,
  qf6_iicmo integer,
  lat_gring text,
  lon_gring text,
  ch_m12 text,
  ch_m13 text,
  file_m07 text,
  file_m08 text,
  file_m10 text,
  file_m12 text,
  file_m13 text,
  file_m14 text,
  file_m15 text,
  file_m16 text,
  file_dnb text,
  file_geo_dnb text,
  file_gmtco text,
  file_gdnbo text,
  file_iicmo text,
  file_ivcop text,
  file_ac text,
  CONSTRAINT detections_pk PRIMARY KEY (pkid)
)
WITH (
  OIDS=FALSE
);
SELECT AddGeometryColumn('detections', 'gmtco_point', 4326, 'POINT', 2);
SELECT AddGeometryColumn('detections', 'dnb_point', 4326, 'POINT', 2);
SELECT AddGeometryColumn('detections', 'gring_poly', 4326, 'POLYGON', 2);
`
	fmt.Printf("%s", createTable)
}

func main() {
	var fname string
	var hasPgid bool
	flag.BoolVar(&hasPgid, "p", false, "Set if first column is pgid.")
	flag.Parse()
	if len(os.Args) < 2 {
		printTableSchema()
		return
	}
	fname = flag.Args()[0]
	file, err := os.Open(fname)
	defer file.Close()
	if nil != err {
		fmt.Errorf("Error failed to open file %s due to %s", fname, err.Error())
		return
	}
	rdr := csv.NewReader(file)
	// Read header
	header, err := rdr.Read()
	if nil != err {
		fmt.Errorf("Error reading header %s", err.Error())
		return
	}

	var rec []string

	first := true

	query := `INSERT INTO detections (
	id,
  id_key,
  date_proc,
  lat_gmtco,
  lon_gmtco,
  date_mscan,
  temp_bb,
  temp_bkg,
  esf_bb,
  rhi,
  rh,
  methane_eq,
  co2_eq,
  area_pixel,
  area_bb,
  cloud_mask,
  qf_fit,
  qf_detect,
  rad_dnb,
  rad_m07,
  rad_m08,
  rad_m10,
  rad_m12,
  rad_m13,
  rad_m14,
  rad_m15,
  rad_m16,
  tran_dnb,
  tran_m07,
  tran_m08,
  tran_m10,
  tran_m12,
  tran_m13,
  tran_m14,
  tran_m15,
  tran_m16,
  pthm_m12,
  pthm_m13,
  pthm_m14,
  pthm_m15,
  pthm_m16,
  qf1_dnb,
  qf1_m07,
  qf1_m08,
  qf1_m10,
  qf1_m12,
  qf1_m13,
  qf1_m14,
  qf1_m15,
  qf1_m16,
  cot_ivcop,
  eps_ivcop,
  qf1_ivcop,
  qf2_ivcop,
  qf3_ivcop,
  dn_m10,
  sample_m10,
  line_m10,
  sample_bt,
  line_bt,
  sample_dnb,
  line_dnb,
  lat_dnb,
  lon_dnb,
  dist_dnb,
  thr_m07,
  thr_m08,
  thr_m10,
  thr_m16,
  solz_gmtco,
  sola_gmtco,
  satz_gmtco,
  sata_gmtco,
  scvx_gmtco,
  scvy_gmtco,
  scvz_gmtco,
  scpx_gmtco,
  scpy_gmtco,
  scpz_gmtco,
  scax_gmtco,
  scay_gmtco,
  scaz_gmtco,
  qf1_gmtco,
  qf2_gmtco,
  qf1_iicmo,
  qf2_iicmo,
  qf3_iicmo,
  qf4_iicmo,
  qf5_iicmo,
  qf6_iicmo,
  lat_gring,
	lon_gring,
  ch_m12,
  ch_m13,
  file_m07,
  file_m08,
  file_m10,
  file_m12,
  file_m13,
  file_m14,
  file_m15,
  file_m16,
  file_dnb,
  file_geo_dnb,
  file_gmtco,
  file_gdnbo,
  file_iicmo,
  file_ivcop,
  file_ac,
	gmtco_point,
	dnb_point,
	gring_poly
	) VALUES `
	var start int
	if hasPgid {
		start = 1
	}
	for rec, err = rdr.Read(); nil == err; rec, err = rdr.Read() {
		var values string
		if !first {
			values += ",\n"
		} else {
			first = false
		}
		values += "("
		rec = rec[start:]
		for i := 0; i < 109; i++ {
			if i == 1 || i == 2 || i == 5 || i >= 90 {
				values += "'" + rec[i] + "', "
			} else {
				values += rec[i] + ", "
			}
		}
		//Lat_GMTCO
		//Lon_GMTCO
		values += fmt.Sprintf("ST_SetSRID(ST_MakePoint(%s, %s), 4326), ", rec[4], rec[3])
		//Lat_DNB
		//Lon_DNB
		values += fmt.Sprintf("ST_SetSRID(ST_MakePoint(%s, %s), 4326), ", rec[63], rec[62])
		//Lat_Gring
		//Lon_Gring
		lons := strings.Split(rec[91], ";")
		lats := strings.Split(rec[90], ";")
		values += "ST_SetSRID(ST_MakePolygon(ST_MakeLine(ARRAY["
		for i := range lons {
			if i >= len(lons) || i >= len(lats) {
				fmt.Printf("Lons: '%s', Lats: '%s', LLons: %d, LLats: %d, HLons: %s, HLats: %s, i: %d\n", rec[91], rec[90], len(lons), len(lats), header[91], header[90], i)
				return
			}
			values += fmt.Sprintf("ST_MakePoint(%s, %s), ", lons[i], lats[i])
		}
		values += fmt.Sprintf("ST_MakePoint(%s, %s)])), 4326))", lons[0], lats[0])
		query = query + values
		fmt.Printf("%s", query)
		query = ""
	}
	query += ";\n"
	fmt.Printf("%s", query)
}
