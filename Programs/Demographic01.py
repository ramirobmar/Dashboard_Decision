import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from collections import namedtuple
import numpy as np 
import pandas as pd 

import etl_function


def function_etl_05(dataset_01,val):

	try:
		columns = dataset_01.columns
		df = dataset_01.pivot_table(values = val, index = [columns[0],'Provincia','Edad'],  columns=['Demografia'],)
		rango = range(len(df))
		
		df['Ambos_sexos'] = np.int32(df['Ambos_sexos'] )
		df['Hombres'] = np.int32(df['Hombres'] )
		df['Mujeres'] = np.int32(df['Mujeres'] )

		
	
		for indice in rango:
			if df['Ambos_sexos'].iloc[indice] == -2147483648:
				df['Ambos_sexos'].iloc[indice] = 0
			if df['Hombres'].iloc[indice] == -2147483648:
				df['Hombres'].iloc[indice] = 0
			if df['Mujeres'].iloc[indice] == -2147483648:
				df['Mujeres'].iloc[indice] = 0
			
		df['Fecha'] = val
			
		
	except Exception as exc:
		print("Excepcion in function_etl_5: {0}".format())	
	
		
	return df

def function_etl_04(dataset_01,val):

	columns = dataset_01.columns
	df = dataset_01.pivot_table(values = val, index = [columns[0],'Provincia','Edad'],  columns=['Demografia'],)
	rango = range(len(df))
	df['Ambos_sexos'] = np.int32(df['Ambos_sexos'] )
	df['Hombres'] = np.int32(df['Hombres'] )
	df['Mujeres'] = np.int32(df['Mujeres'] )
	
	for indice in rango:
		if df['Ambos_sexos'].iloc[indice] == -2147483648:
			df['Ambos_sexos'].iloc[indice] = 0
		if df['Hombres'].iloc[indice] == -2147483648:
			df['Hombres'].iloc[indice] = 0
		if df['Mujeres'].iloc[indice] == -2147483648:
			df['Mujeres'].iloc[indice] = 0
	return df
	
def function_etl_01(dataset_01):
	print("###########################################################################")
	print("01: Starting Function of ETL for convertion of Fields Province and Code ")
	numcols  = len(dataset_01.Demografia)
	edad = 0	
	for index in range(numcols):
		regs = dataset_01.Demografia[index].split()
		if len(regs) >= 2 and regs[1]!="anyos":
			
			if len(regs) != 2:
				cadena =  ""
				for i in range(len(regs)-1):
					cadena = "{0} {1}".format(cadena,regs[i+1])
			else:
				cadena = regs[1]
			
			dataset_01.loc[index+1,'Codigo'] = "{0}".format(regs[0])
			dataset_01.loc[index+1,'Provincia'] = cadena
			dataset_01.loc[index+1,'Edad'] = "{0}".format(edad)
			
			dataset_01.loc[index+2,'Codigo'] = "{0}".format(regs[0])
			dataset_01.loc[index+2,'Provincia'] = cadena
			dataset_01.loc[index+2,'Edad'] = "{0}".format(edad)
			
			dataset_01.loc[index+3,'Codigo'] = "{0}".format(regs[0])
			dataset_01.loc[index+3,'Provincia'] = cadena
			dataset_01.loc[index+3,'Edad'] = "{0}".format(edad)
			
		else:
			 if  len(regs) == 2:
			 		if regs[1] == "anyos":
			 			edad = edad + 1	
			
	print("02: Ending Function of ETL for convertion of Fields Province and Code ")
	print("###########################################################################")
	
	return dataset_01

def function_etl_02(dataset_01):
	print("###########################################################################")
	print("01: Starting Function of records cleaning ")
	numcols  = len(dataset_01.Demografia)
	dataset_02=pd.DataFrame(columns=dataset_01.columns)
	for index in range(numcols):
		regs = dataset_01.Demografia[index].split()
		if len(regs) != 2:
			dataset_02 = dataset_02.append(dataset_01.loc[index],ignore_index=True)
			
	print("02: Ending Function of records cleaning")
	print("###########################################################################")
	
	return dataset_02

def function_etl_03(dataset_01):
	
	print("###########################################################################")
	print("01: Starting function of data format in records")
	size = len(dataset_01)
	columns = analysis_1.columns[2:size-3]
	try:
		for index in columns:
			for cols in range(len(dataset_01[index])):
				registro = float(analysis_1[index].loc[cols])
				if registro >= 0:
					analysis_1[index].loc[cols] = int(registro)
				else:
					analysis_1[index].loc[cols] = 0
					
	except Exception as exc:
		print("Excection in {0} as {1}".format(index,exc))
	finally:
		print("02: Ending function of data format in records")
		print("###########################################################################")
	
	return dataset_01


if __name__ == "__main__":
	
	print("#############################################################################")
	print("Demographic Study - Demographic Study in Spain")
	print("#############################################################################")
		
	try:
		
		#namefile_1 = "../Datasets/Poblacion.csv"
		#analysis_1 = pd.read_csv(namefile_1,  sep=';', low_memory=False)
		#print("Dataset readed ....")
		#print("Comienzo del proceso de Extraccion y Transformacion de la Informacion FASE 1")
		#analysis_2 = function_etl_01(analysis_1)
		#analysis_3 = function_etl_02(analysis_2)
		#analysis_3.to_csv( path_or_buf="../Datasets/Poblacion_Regs_Cleaning.csv", sep=';')
		#print("Finalizacion del proceso de Extraccion y Transformación de la Informacion FASE 1")
		#print("Comienzo del proceso de Limpieza de registros")
		
		namefile_1 = "../Datasets/Poblacion_Regs_Cleaning.csv"
		analysis_1 = pd.read_csv(namefile_1,  sep=';', low_memory=False)
		size = len(analysis_1)
		columns = analysis_1.columns[2:34]
		for index in columns:
			print("Columns: {0}".format(index))
			analysis_1[index]=np.int32(analysis_1[index])
		print("Finalizacion del proceso de Limpieza de registros")
		print("Comienzo del proceso de Extraccion y Transformacion de la Informacion FASE 2")
		analysis_1.to_csv( path_or_buf="../Datasets/Poblacion_Regs_Cleaning_01.csv", sep=';')
		
		df = function_etl_05(analysis_1, '01/01/14')
		df1 = function_etl_05(analysis_1, '01/01/15')
		df = df.append(df1)
		df1 = function_etl_05(analysis_1, '01/01/16')
		df = df.append(df1)
		df1 = function_etl_05(analysis_1, '01/01/17')
		df = df.append(df1)
		
		df.to_csv( path_or_buf="../Datasets/Poblacion_Regs_Cleaning_03.csv", sep=';')
		
		#print("Saving 01/01/14")
		#analysis_5.to_excel( excel_writer="../Datasets/Poblacion_Regs_Cleaning_02.xlsx", sheet_name='01/01/14')
		#analysis_5 = function_etl_04(analysis_1, '01/01/15')
		#print("Saving 01/01/15")
		#analysis_5.to_excel( excel_writer="../Datasets/Poblacion_Regs_Cleaning_02.xlsx", sheet_name= '01/01/15')
		#analysis_5 = function_etl_04(analysis_1, '01/01/16')
		#print("Saving 01/01/16")
		#analysis_5.to_excel( excel_writer="../Datasets/Poblacion_Regs_Cleaning_02.xlsx", sheet_name= '01/01/16')
		#analysis_5 = function_etl_04(analysis_1, '01/01/17')
		#print("Saving 01/01/17")
		#analysis_5.to_excel( excel_writer="../Datasets/Poblacion_Regs_Cleaning_02.xlsx", sheet_name= '01/01/17')
		
		print("Finalizacion del proceso de Extraccion y Transformacion de la Informacion FASE 2")
		print("Done Data Conversion ......")
		print("Exit .....")
		
	except Exception as exc:
		print("Excepcion: {0}".format(exc))
	finally:
		sys.exit(0)

