

with open('/home/benito.ramos/Desktop/GIT/ApacheBeam/inputs/historic.txt' ) as f:
    lines = f.readlines()

    cantidad_ant = 0
    fechas_ant   = ""
    salida = list()
    notsalida = list()
    
    for i in lines:
        clean_line = i.rstrip("\n").split("\t")
        fecha = clean_line[0]
        cantidad = int(clean_line[1])

        if cantidad <= 200000000:
            if cantidad_ant  +  cantidad <= 200000000:
                cantidad_ant += cantidad
                fechas_ant   += ',' + "'" + fecha + "'"
            else:
                salida.append([fechas_ant, cantidad_ant])

                cantidad_ant = cantidad
                fechas_ant = "'" + fecha + "'"
        else:
            notsalida.append([fecha, cantidad])

    salida.append([fechas_ant, cantidad_ant])
        
with open('/home/benito.ramos/Desktop/GIT/ApacheBeam/outputs/read.txt', 'w') as file:
    file.writelines("INSERT INTO WHOWNER_TBL_DESA.BT_VISITS_HIVE_FULL_PART" + str(count) + " SELECT * FROM WHOWNER_TBL_DESA.BT_VISITS_HIVE_FULL_COPY WHERE CAST(AUD_UPD_DT AS DATE) IN (" + i[0] + "); \n" for count, i in enumerate(salida))



print(notsalida)
        
            

# with open('/home/benito.ramos/Desktop/GIT/ApacheBeam/outputs/tablas.txt', 'w') as file:

#     for i in range(0, 341):
#         file.writelines(


#     "CREATE MULTISET TABLE WHOWNER_TBL_DESA.BT_VISITS_HIVE_FULL_PART" + str(i) + """ ,FALLBACK ,
#         NO BEFORE JOURNAL,
#         NO AFTER JOURNAL,
#         CHECKSUM = DEFAULT,
#         DEFAULT MERGEBLOCKRATIO,
#         MAP = TD_MAP6
#         (
#         SIT_SITE_ID CHAR(3) CHARACTER SET LATIN CASESPECIFIC NOT NULL,
#         ITE_ITEM_ID DECIMAL(10,0) NOT NULL,
#         TIM_DAY DATE FORMAT 'yyyy-mm-dd',
#         ITE_SHIPPING_PAYMENT_TYPE VARCHAR(255) CHARACTER SET LATIN CASESPECIFIC COMPRESS ,
#         ITE_SHIPPING_MODE VARCHAR(50) CHARACTER SET LATIN CASESPECIFIC COMPRESS ,
#         CUS_CUST_ID_SEL DECIMAL(12,0) DEFAULT 9.  COMPRESS ,
#         CAT_CATEG_ID_L1 VARCHAR(6) CHARACTER SET LATIN CASESPECIFIC NOT NULL COMPRESS ('1648','1430','1798','1051','1000','1276','1574','1132','1144','1743','1039','5672','1168','1246','3025','3937','1747','1953','1367','1182','5725','1499','1196','1384','1071','5726','3281','1540','1459','2547','1368','1740','28','10386','5973','232','164','112','245','196','8914','101','9256'),
#         CAT_CATEG_ID_L7 VARCHAR(6) CHARACTER SET LATIN CASESPECIFIC COMPRESS ,
#         CBO_COMBO_CLASSI_ID DECIMAL(4,0) NOT NULL DEFAULT -1.  COMPRESS (1. ,2. ,3. ,4. ,5. ,6. ,7. ,8. ,9. ,10. ,11. ,12. ,13. ,14. ,-1. ),
#         CBO_COMBO_ID CHAR(2) CHARACTER SET LATIN CASESPECIFIC NOT NULL DEFAULT '9 ' COMPRESS ('0 ','1 ','2 ','3 ','4 ','6 ','9 ','A ','B ','C ','D ','E ','G ','P ','R ','S '),
#         CNS_CONCESIONARIO_FLAG DECIMAL(1,0) NOT NULL DEFAULT 0.  COMPRESS (0. ,1. ),
#         ITE_TIPO_PROD CHAR(1) CHARACTER SET LATIN CASESPECIFIC NOT NULL DEFAULT '9' COMPRESS ('9','=','N','S','U','s'),
#         ITE_ZIP_CODE VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS ,
#         DEVICE VARCHAR(50) CHARACTER SET LATIN CASESPECIFIC COMPRESS '1',
#         PLATFORM VARCHAR(100) CHARACTER SET LATIN CASESPECIFIC COMPRESS ,
#         ITE_OFFICIAL_STORE_ID DECIMAL(19,0) COMPRESS ,
#         ITE_MP_FLAG CHAR(1) CHARACTER SET LATIN CASESPECIFIC COMPRESS ('M','N','X','Y'),
#         QTY_VISITS DECIMAL(10,0) COMPRESS ,
#         QTY_PAGEVIEWS DECIMAL(10,0) COMPRESS ,
#         QTY_VISITS_CATALOG DECIMAL(10,0) COMPRESS ,
#         QTY_PAGEVIEWS_CATALOG DECIMAL(10,0) COMPRESS ,
#         AUD_INS_DT TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP(0) COMPRESS ,
#         AUD_UPD_DT TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP(0) COMPRESS ,
#         AUD_FROM_INTERFACE VARCHAR(60) CHARACTER SET LATIN CASESPECIFIC)
#     PRIMARY INDEX ( SIT_SITE_ID ,ITE_ITEM_ID )
#     PARTITION BY RANGE_N(TIM_DAY  BETWEEN DATE '2015-01-01' AND DATE '2025-12-31' EACH INTERVAL '1' DAY ,
#     NO RANGE, UNKNOWN); \n
#     """

#         )