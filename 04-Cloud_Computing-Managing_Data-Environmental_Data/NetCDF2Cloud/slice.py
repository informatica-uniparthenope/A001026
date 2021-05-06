from netCDF4 import Dataset
from os import mkdir, path, remove, makedirs
import numpy as np

from time import perf_counter

import json
from sys import exit

import sys

if not sys.warnoptions:
    import os, warnings

    warnings.simplefilter("ignore", RuntimeWarning)  # Change the filter in this process


def calc_divisors(num):
    result = []
    for x in range(1, num + 1):
        if num % x == 0:
            result.append(x)
    return result


if __name__ == "__main__":

    filename = "data/rms3_d03_20200506Z1200.nc"

    rootgrp = Dataset(filename)

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

    print("Dizionario dei sinonimi delle latitudini: " + str(dict_lat))
    print("Dizionario dei sinonimi delle longitudini: " + str(dict_lon))
    print("Dizionario dei sinonimi del tempo: " + str(dict_time))

    print("\nDivisori:")
    dimensions_divisors = {}
    for dimension in rootgrp.dimensions.values():
        dimensions_divisors[str(dimension.name)] = calc_divisors(len(dimension))
        print(str(dimension.name), dimensions_divisors[str(dimension.name)])

    # In letteratura so che è 8MB è un valore ottimo con cui lavorare
    center = 8196
    time = 1
    level = 1
    dtype = 4
    sizes = []

    # Convenzione = prima latitudine e poi longitudine
    Y_DIM = "NONE"
    X_DIM = "NONE"
    Z_DIM = "NONE"  # TIME
    Z_SIZE = 0

    # Stampa corrispondenze trovate
    dimensions = {}
    for dimension in rootgrp.dimensions.values():
        if str(dimension.name) in dict_lat:
            print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi delle latitudini.")
            Y_DIM = str(dimension.name)
        if str(dimension.name) in dict_lon:
            print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi delle longitudini.")
            X_DIM = str(dimension.name)
        if str(dimension.name) in dict_time:
            print("Trovata corrispondenza '" + str(dimension.name) + "' relativa ai sinonimi del tempo.")
            Z_DIM = str(dimension.name)
            Z_SIZE = dimension.size

    if (Y_DIM == "NONE"):
        print("Non è stata trovata alcuna corrispondenza per la latitudine.")
        print("Aggiornare il dizionario dei sinonimi.")

    if (X_DIM == "NONE"):
        print("Non è stata trovata alcuna corrispondenza per la longitudine.")
        print("Aggiornare il dizionario dei sinonimi.")

    if (Z_DIM == "NONE"):
        print("Non è stata trovata alcuna corrispondenza per il tempo.")
        print("Aggiornare il dizionario dei sinonimi.")

    print("\n")
    y_size = dimensions_divisors[Y_DIM][-1]
    x_size = dimensions_divisors[X_DIM][-1]
    xy_size = y_size * x_size * dtype
    print("Dimensionalità '" + str(Y_DIM) + "': " + str(y_size))
    print("Dimensionalità '" + str(X_DIM) + "': " + str(x_size))

    print("Dimensionalità moltiplicate: " + str(xy_size))

    # Start the stopwatch / counter
    t1_start = perf_counter()

    # Algoritmo per sizes
    for j in range(0, len(dimensions_divisors[Y_DIM])):
        new = []
        m = dimensions_divisors[Y_DIM][j]
        for i in range(0, len(dimensions_divisors[X_DIM])):
            n = dimensions_divisors[X_DIM][i]
            q = xy_size / (n * m)
            f = abs(center - q)
            # print(m, n, q, f)

            new.append(f)

        sizes.append(new)

    # Sizes contiene lo spazio variabile in tutte le possibili coppie di divisori
    # print("\nSizes:")
    # print(sizes)

    minVal = 1E37
    j0 = -1
    i0 = -1

    # Per evitare j = 0 e i = 0
    # Visto che sizes ora va da 0 a N
    for j in range(1, len(sizes)):
        for i in range(1, len(sizes[j])):
            # print (sizes[j][i], minVal)
            if sizes[j][i] < minVal:
                minVal = sizes[j][i]
                j0 = j
                i0 = i

    print("\nminVal: " + str(minVal))
    print(j0, i0, dimensions_divisors[Y_DIM][j0], dimensions_divisors[X_DIM][i0])

    # Divisori della lat. e long.
    div_lat_y = dimensions_divisors[Y_DIM][j0]
    div_lon_x = dimensions_divisors[X_DIM][i0]

    # Numero di divisioni tali che ci sia proprozionalità rispetto agli 8MB prefissati
    print("\nNumero di split per la latitudine: " + str(div_lat_y))
    print("Numero di split per la longitudine: " + str(div_lon_x))
    print("Numero di split per il tempo: " + str(Z_SIZE))

    # Salvo le dimensioni di lat. e long. dal rootgrp
    dimension_names = ()
    for dimension_name in rootgrp.dimensions:
        dimension_names = dimension_names + (dimension_name,)

    for dimension in rootgrp.dimensions.values():
        if str(dimension.name) in dimension_names:
            if (dimension.name in dict_lat):
                num_lat = dimension.size
            if (dimension.name in dict_lon):
                num_lon = dimension.size

    # Massimo numero di passi (iterazioni) da effettuare nella ripartizione (splitting) di una
    # variabile in più parti
    # Calcolato grazie ai divisori della lat. e long.
    steps = int(Z_SIZE * (div_lat_y * div_lon_x))

    print("\nNumero di split totali (per variabile): " + str(steps))

    # Dimensione degli split relativi a lat e long
    split_size_lat_y = int(num_lat / div_lat_y)
    split_size_lon_x = int(num_lon / div_lon_x)

    print("\nDimensionalità degli split per la latitudine: " + str(split_size_lat_y))
    print("Dimensionalità degli split per la longitudine: " + str(split_size_lon_x))
    print("Dimensionalità degli split per il tempo: " + str(Z_SIZE))

    # Crea liste e azzerale
    start_lat = [0] * div_lat_y
    end_lat = [0] * div_lat_y
    start_lon = [0] * div_lon_x
    end_lon = [0] * div_lon_x

    for ind in range(0, div_lat_y):
        start_lat[ind] = (ind * split_size_lat_y)
        end_lat[ind] = ((split_size_lat_y * (ind + 1)) - 1)

    for ind in range(0, div_lon_x):
        start_lon[ind] = (ind * split_size_lon_x)
        end_lon[ind] = ((split_size_lon_x * (ind + 1)) - 1)

    print("\n\nRange della dimensionalità di ogni file splittato:")

    print("\nNum: [start_lat, end_lat]")

    for cnt in range(0, len(start_lat)):
        print(str(cnt) + ": [" + str(start_lat[cnt]) + ", " + str(end_lat[cnt]) + "]")

    print("\nNum: [start_lon, end_lon]")

    for cnt in range(0, len(start_lon)):
        print(str(cnt) + ": [" + str(start_lon[cnt]) + ", " + str(end_lon[cnt]) + "]")

    """
        Creazione del file copia dall'originale (con dovute modifiche).
        In tale file saranno riportati tutte le dimensionalità, le variabili e gli attributi
        del file originario, più tutti i dati delle variabili che hanno singola dimensionalità (ovvero time (time),
        longitude (longitude) e latitude (latitude)
    """

    base_path = "data/" + path.splitext(path.basename(filename))[0]
    if not path.exists(base_path):
        makedirs(base_path)

    file_path = base_path + "/__meta__.nc4"

    dataset = Dataset(file_path, "w", format="NETCDF4")

    # Copia delle dimensioni
    for dimension in rootgrp.dimensions.values():
        dim = dataset.createDimension(str(dimension.name), dimension.size)
        dimensions[dimension.name] = dim

    # Copia degli attributi globali
    for name in rootgrp.ncattrs():

        # Se c'è _NCProperties vuol dire che non ci sono attributi globali
        if (name != "_NCProperties"):
            dataset.setncattr(name, getattr(rootgrp, name))

    # Copia delle variabili
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

        # Se non esiste l'attributo
        if (attr_fillValue == False):
            temp = dataset.createVariable(variable.name, variable.dtype, dimension_names, zlib=True)

        # Se l'attributo esiste
        if (attr_fillValue != False):
            temp = dataset.createVariable(variable.name, variable.dtype, dimension_names, zlib=True,
                                          fill_value=attr_fillValue)

        # time, latitude, longitude
        # Conta le presenze di tali dimensioni nelle variabili
        flag_dimension_count = [0, 0, 0]

        # Controllo se nella variabile attuale ci sono almeno le dimensioni di lat. e long.
        # Controllo time, lat e lon
        for dimension_name in variable.dimensions:
            if (dimension_name == str(Z_DIM)):
                flag_dimension_count[0] = 1
            if (dimension_name == str(Y_DIM)):
                flag_dimension_count[1] = 1
            if (dimension_name == str(X_DIM)):
                flag_dimension_count[2] = 1

        # Se il secondo ed il terzo elemento di flag_dimension_count sono uguali
        # a 1, significa che è una variabile che successivamente dovrà essere splittata.
        # Quindi in questa copia del file originario i suoi dati non dovranno essere riportati.
        # Vengono copiati solo i dati relativi alle variabili del tipo "time (time), latitude (latitude), longitude (longitude)".

        # Copia contenuto variabile solo se è una variabile che riporta la dimensione
        # Basta che NON siano presenti contemporaneamente le dimensioni time, lat e lon, oppure solo lat e lon.
        if (not (flag_dimension_count[0] == 1 and flag_dimension_count[1] == 1 and flag_dimension_count[1] == 1 or
                 flag_dimension_count[1] == 1 and flag_dimension_count[2] == 1)):
            temp[:] = rootgrp.variables[variable.name][:]


        else:
            # Creazione attributi relativi agli split (solo per queste variabili)
            temp._cos_split_latitude = np.int32(split_size_lat_y)
            temp._cos_split_longitude = np.int32(split_size_lon_x)

        # Copia il resto degli attributi
        for k in variable.ncattrs():
            if (k != "_FillValue"):
                temp.setncattr(k, variable.getncattr(k))

    dataset.close()

    # Inizio algoritmo per splittare le variabili
    """
        # Versione 1 dell'algoritmo di splitting
        # Vengono splittate solo le variabili che hanno come argomenti latitudine (o sinonimi)
        # e longitudine (o sinonimi), evitando quindi le variabili con 1 dimensione, le variabili
        # che seguono lo standard cf risultando esplicate sia come dimensione che
        # come variabile, es: time, long, lat.
    """

    # Contatore per stampare a video la numerazione (sequenziale) della variabile in cui mi trovo
    # Just debug.
    numero_var_print = 0
    variables = []
    for variable in rootgrp.variables.values():

        numero_var_print = numero_var_print + 1
        print("\n" + str(numero_var_print) + ")")

        # Ottieni info sulla variabile
        print(variable)

        # Dimensioni della variabile
        dimension_names = ()
        for dimension_name in variable.dimensions:
            dimension_names = dimension_names + (dimension_name,)

        # -----------------------------------
        attributes = []
        for attribute in variable.ncattrs():
            attributes.append({"name": str(attribute), "value": str(variable.getncattr(attribute))})

        shapes = []
        for shape in variable.shape:
            shapes.append(shape)

        flag_dimension_count = [0, 0, 0]
        # Controllo se nella variabile attuale ci sono le dimensioni di lat. e long.
        for dimension_name in variable.dimensions:
            if (dimension_name == str(Z_DIM)):
                flag_dimension_count[0] = 1
            if (dimension_name == str(Y_DIM)):
                flag_dimension_count[1] = 1
            if (dimension_name == str(X_DIM)):
                flag_dimension_count[2] = 1

        # Eseguiamo l'algoritmo solo se sono presenti contemporaneamente le dimensioni time, lat e lon, oppure solo lat e lon.
        if ((flag_dimension_count[0] == 1 and flag_dimension_count[1] == 1 and flag_dimension_count[1] == 1 or
             flag_dimension_count[1] == 1 and flag_dimension_count[2] == 1)):

            # Algoritmo eseguito Z_SIZE volte (time)
            for count_time in range(1, Z_SIZE + 1):

                # Base path già creato

                """
                        # cartella var/0 = time
                        # cartella var/0/0 = latitudini
                        # file della cartella var/0/0/ sono longitudini
                        #   es: var/0/0/0.nc => 0.nc longitudine                       
                """

                # Crea path per la variabile
                variable_path = base_path + "/" + str(variable.name)
                if not path.exists(variable_path):
                    makedirs(variable_path)

                # Crea path per il tempo
                time_path = variable_path + "/" + str(count_time - 1)
                if not path.exists(time_path):
                    makedirs(time_path)

                # Algoritmo eseguito div_lat_y volte (latitudine)
                for count_lat in range(1, div_lat_y + 1):

                    lat_path = time_path + "/" + str(count_lat - 1)
                    if not path.exists(lat_path):
                        makedirs(lat_path)

                    # Algoritmo eseguito div_lon_x volte (longitudine)
                    for count_lon in range(1, div_lon_x + 1):

                        lon_path_file = lat_path + "/" + str(count_lon - 1) + ".nc4"

                        if path.exists(lon_path_file):
                            remove(lon_path_file)

                        dataset = Dataset(lon_path_file, "w", format="NETCDF4")

                        """
                            Le variabili dei file netCDF, hanno sempre come due ultimi dimensioni la
                            longitudine e latitudine (tranne per variabili semplici come time, lat, long, che 
                            non vengono considerate in ogni caso) e come primo argomento la dimensione tempo.
                        """

                        # Se siamo in una variabile col tempo
                        if (flag_dimension_count[0] == 1):

                            # E se le dimensioni sono 3
                            if (len(dimension_names) == 3):
                                slice_t = variable[count_time - 1,  # dimensionalità tempo
                                          start_lat[count_lat - 1]:end_lat[count_lat - 1] + 1,  # seconda dimensione
                                          start_lon[count_lon - 1]:end_lon[count_lon - 1] + 1]  # terza dimensione


                            # Altrimenti sono più di 3 dimensioni
                            else:
                                slice_t = variable[count_time - 1,  # dimensionalità tempo
                                          ...,  # dimensioni da [1 a ultima-2]
                                          start_lat[count_lat - 1]:end_lat[count_lat - 1] + 1,  # penultima dimensione
                                          start_lon[count_lon - 1]:end_lon[count_lon - 1] + 1]  # ultima dimensione

                        # Se siamo in una variabile con lat e lon (senza tempo)
                        else:

                            if (len(dimension_names) == 2):

                                slice_t = variable[start_lat[count_lat - 1]:end_lat[count_lat - 1] + 1,
                                          # prima dimensione
                                          start_lon[count_lon - 1]:end_lon[count_lon - 1] + 1]  # seconda dimensione
                            else:
                                slice_t = variable[...,  # dimensioni da [0 a ultima-2]
                                          start_lat[count_lat - 1]:end_lat[count_lat - 1] + 1,  # penultima dimensione
                                          start_lon[count_lon - 1]:end_lon[count_lon - 1] + 1]  # ultima dimensione

                        # Crea dimensioni
                        dimension_names = ()
                        for dimension_name in variable.dimensions:
                            dimension_names = dimension_names + (dimension_name,)

                        dimensions = {}
                        # print("Dimension var of '" + str(variable.name) + "'")
                        for dimension in rootgrp.dimensions.values():
                            if str(dimension.name) in dimension_names:
                                # print(dimension.name, dimension.size)

                                if ((str(dimension.name) == str(Y_DIM))):

                                    # La latitudine è notoriamente la penultima posizione
                                    dim = dataset.createDimension(str(dimension.name), slice_t.shape[-2])


                                elif ((str(dimension.name) == str(X_DIM))):

                                    # La longitudine invece è l'ultima posizione
                                    dim = dataset.createDimension(str(dimension.name), slice_t.shape[-1])

                                elif ((str(dimension.name) == str(Z_SIZE))):

                                    # Il tempo ha sempre dimensione uguale a 1 nei file splittati
                                    # Sia che la dimensione originaria del file di partenza sia 1 o meno
                                    dim = dataset.createDimension(str(dimension.name), 1)

                                else:
                                    # Dimensione non relativa a lat o lon, quindi recuperabile dal file originario
                                    dim = dataset.createDimension(str(dimension.name), dimension.size)

                                dimensions[dimension.name] = dim

                        # Crea variabile - variable_path_file_name o variable.name?  Gruppo o no?

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

                        temp[:] = slice_t[:]

                        # Copia il resto degli attributi
                        for k in variable.ncattrs():
                            if (k != "_FillValue"):
                                temp.setncattr(k, variable.getncattr(k))

                        # print (temp.shape)
                        # exit(0)
                        # print (dimension_names)
                        # print (slice_t.shape)
                        # print (variable.shape)

                        """ debug:
                        print (variable.shape)
                        print (variable.size)

                        print (slice_t.shape)
                        print (slice_t.size)
                        """

                        """
                        print("\nLista info: ")
                        print("name: " + str(variable.name))
                        print("dtype: " + str(variable.dtype))
                        print("nndim: " + str(variable.ndim))
                        print("nshape: " + str(shapes))
                        print("ndimensions: " + str(dimension_names))
                        print("nattributes: " + str(attributes))
                        """

                        dataset.close()

    # Stop the stopwatch / counter
    t1_stop = perf_counter()

    print("Durata slice in secondi: ", t1_stop - t1_start)
