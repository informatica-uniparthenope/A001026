import shutil

from netCDF4 import Dataset
import numpy as np

def merge(dataset_path, metafile, filename, var_list, round_start_lat, round_end_lat, round_start_lon, round_end_lon, _cos_split_latitude, _cos_split_longitude):
    folder_lat_start = int(round_start_lat / _cos_split_latitude)
    folder_lat_end = int(round_end_lat / _cos_split_latitude)
    file_lon_start = int(round_start_lon / _cos_split_longitude)
    file_lon_end = int(round_end_lon / _cos_split_longitude)

    print("filename: " + str(filename))
    #print(metafile)
    #print(var_list)
    rootgrp = Dataset(metafile)

    dict_lat = ()
    dict_lon = ()
    dict_time = ()

    # Dizionario latitudini
    file_lat = open("dictionary/lat.txt", "r")
    for line in file_lat.readlines():
        dict_lat = dict_lat + (line.rstrip(),)
    file_lat.close()

    # Dizionario latitudini
    file_long = open("dictionary/lon.txt", "r")
    for line in file_long.readlines():
        dict_lon = dict_lon + (line.rstrip(),)
    file_long.close()

    # Dizionario tempo
    file_time = open("dictionary/time.txt", "r")
    for line in file_time.readlines():
        dict_time = dict_time + (line.rstrip(),)
    file_time.close()

    # Convenzione = prima latitudine e poi longitudine
    Y_DIM = "NONE"
    Y_SIZE = 0
    X_DIM = "NONE"
    X_SIZE = 0
    Z_DIM = "NONE"  # TIME
    Z_SIZE = 0

    # print ("")
    # Stampa corrispondenze trovate
    dimensions = {}
    for dimension in rootgrp.dimensions.values():
        if str(dimension.name) in dict_lat:
            # print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi delle latitudini.")
            Y_DIM = str(dimension.name)
            Y_SIZE = dimension.size
        if str(dimension.name) in dict_lon:
            # print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi delle longitudini.")
            X_DIM = str(dimension.name)
            X_SIZE = dimension.size
        if str(dimension.name) in dict_time:
            # print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi del tempo.")
            Z_DIM = str(dimension.name)
            Z_SIZE = dimension.size

    numero_cartelle = folder_lat_end - folder_lat_start + 1
    numero_file = file_lon_end - file_lon_start + 1

    numero_file_totali = numero_cartelle * numero_file

    """
    print ("\nNumero di cartelle: " + str(numero_cartelle))
    print ("Numero di file in una cartella: " + str(numero_file))
    print ("Numero di file totali: " + str(numero_file_totali))
    """

    new_lat_dim = ((folder_lat_end - folder_lat_start) + 1) * _cos_split_latitude
    new_lon_dim = ((file_lon_end - file_lon_start) + 1) * _cos_split_longitude

    dataset = Dataset(filename, "w", format="NETCDF4")

    # Ora fill dei dati della variabile scelta

    # Dimensioni globali
    dimensions = {}
    for dimension in rootgrp.dimensions.values():
        if ((str(dimension.name) == str(Y_DIM))):
            dim = dataset.createDimension(str(dimension.name), new_lat_dim)

        elif ((str(dimension.name) == str(X_DIM))):
            dim = dataset.createDimension(str(dimension.name), new_lon_dim)

        else:
            # Dimensione non relativa a lat o lon, quindi recuperabile dal file originario
            dim = dataset.createDimension(str(dimension.name), dimension.size)

        dimensions[dimension.name] = dim

    # Copia degli attributi globali
    for name in rootgrp.ncattrs():

        # Se c'è _NCProperties vuol dire che non ci sono attributi globali
        if (name != "_NCProperties"):
            dataset.setncattr(name, getattr(rootgrp, name))

    # Variabili
    for variable in rootgrp.variables.values():

        # Dimensioni della variabile
        dimension_names = ()
        for dimension_name in variable.dimensions:
            dimension_names = dimension_names + (dimension_name,)

        """
        C'è un problema con gli attributi _fillValue e missingValue.
        Entrambi non possono essere gestiti dagli utenti in modo normale.
        Bisogna creare tali attributi "ad hoc" ogni volta che si crea una variabile.
        """

        attr_fillValue = False

        # Ottengo l'attributo _FillValue (se esiste)
        for k in variable.ncattrs():
            if (k == "_FillValue"):
                attr_fillValue = variable.getncattr(k)

        # Se non esiste l'attributo crea regolarmente
        if (attr_fillValue == False):
            temp = dataset.createVariable(variable.name, variable.dtype, dimension_names, zlib=True)

        # Se l'attributo esiste
        if (attr_fillValue != False):
            temp = dataset.createVariable(variable.name, variable.dtype, dimension_names, zlib=True,
                                          fill_value=attr_fillValue)

        #final_path = "./" + filename.split("_lat")[0].replace("output", "data")
        final_path = "./" + filename.split("_lat")[0]
        #print(final_path)
      
        # Se la variabile attuale è quella scelta dall'utente
        if variable.name in var_list:

            for ind_j in range(folder_lat_start, folder_lat_end + 1):
                for ind_i in range(file_lon_start, file_lon_end + 1):

                    temp_dataset = Dataset(final_path + "/" + "/0/" \
                                           + str(ind_j) + "/" + str(ind_i) + ".nc4")

                    num_dimensioni = len(dimension_names)

                    # Se si tratta di un solo file in totale
                    if numero_file_totali == 1:
                        var_finale = temp_dataset.variables[variable.name][:]

                    # Se sono più file totali
                    else:

                        # Se sono più file totali, ma 1 solo file di longitudine (per cartella, latitudine)
                        if numero_file == 1:

                            # Se siamo alla prima cartella
                            if ind_j == folder_lat_start:
                                var_finale = temp_dataset.variables[variable.name][:]


                            # Se abbiamo già superato la prima cartella
                            else:
                                var_finale = np.concatenate([var_finale, temp_dataset.variables[variable.name][:]],
                                                            axis=num_dimensioni - 2)

                        # Se ci sono più file, ma una cartella
                        elif numero_cartelle == 1:

                            # Se siamo al primo file
                            if ind_i == file_lon_start:
                                var_finale = temp_dataset.variables[variable.name][:]

                            # Se abbiamo già superato il primo file
                            else:
                                var_finale = np.concatenate([var_finale, temp_dataset.variables[variable.name][:]],
                                                            axis=num_dimensioni - 1)

                        # Se ci sono più file e più cartelle
                        elif numero_file > 1 and numero_cartelle > 1:

                            # Se siamo alla prima cartella
                            if ind_j == folder_lat_start:

                                # Se siamo al primo file
                                if ind_i == file_lon_start:
                                    var_parziale = temp_dataset.variables[variable.name][:]

                                # Se abbiamo già superato il primo file (ma non siamo all'ultimo)
                                elif ind_i < file_lon_end:
                                    var_parziale = np.concatenate(
                                        [var_parziale, temp_dataset.variables[variable.name][:]],
                                        axis=num_dimensioni - 1)

                                # Se siamo all'ultimo file
                                else:
                                    var_parziale = np.concatenate(
                                        [var_parziale, temp_dataset.variables[variable.name][:]],
                                        axis=num_dimensioni - 1)

                                    var_parziale_cartella = var_parziale

                            # Siamo alle cartelle successive
                            else:
                                # Se siamo al primo file
                                if ind_i == file_lon_start:
                                    var_parziale = temp_dataset.variables[variable.name][:]

                                # Se abbiamo già superato il primo file (ma non siamo all'ultimo)
                                elif ind_i < file_lon_end:
                                    var_parziale = np.concatenate(
                                        [var_parziale, temp_dataset.variables[variable.name][:]],
                                        axis=num_dimensioni - 1)

                                # Se siamo all'ultimo file
                                else:

                                    # parziale di tutti i file lon della cartella
                                    var_parziale = np.concatenate(
                                        [var_parziale, temp_dataset.variables[variable.name][:]],
                                        axis=num_dimensioni - 1)

                                    # parziale di tutti i file della cartella attuale e quella precedente
                                    var_parziale_cartella = np.concatenate([var_parziale_cartella, var_parziale],
                                                                           axis=num_dimensioni - 2)

                                    # aggiorna var_finale
                                    var_finale = var_parziale_cartella
            temp[:] = var_finale[:]

        # Se è una variabile diversa (da quella scelta dall'utente)
        else:

            # Se è variabile time
            if variable.name == Z_DIM:
                temp[:] = rootgrp.variables[variable.name][:]

            # Se è la lat
            elif variable.name == Y_DIM:
                temp[:] = variable[round_start_lat:round_end_lat + 1]

            # Se è la lon
            elif variable.name == X_DIM:
                temp[:] = variable[round_start_lon:round_end_lon + 1]

            # Se è una qualsiasi altra variabile dimension
            elif variable.name in dimension_names:
                temp[:] = rootgrp.variables[variable.name][:]

            # Se è qualsiasi altra variabile non bisogna fare nulla

        # Copia il resto degli attributi
        for k in variable.ncattrs():
            if (k != "_FillValue"):
                temp.setncattr(k, variable.getncattr(k))

    deleteTempPath = "download/" + dataset_path + "/" + str(var_list[0])

    try:
        shutil.rmtree(deleteTempPath)
        print ("\nDirectory temporanea [" + str(deleteTempPath) + "] rimossa con successo!")
    except:
        print("\nErrore! Non riesco ad eliminare la directory temporanea: " + str(deleteTempPath))

    # print ("\nFile risultante come [" + file_merged_path + "] creato con successo!")
    dataset.close()
