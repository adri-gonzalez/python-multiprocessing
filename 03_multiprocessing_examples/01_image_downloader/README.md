# IMAGE DOWNLOADER MULTIPROCESSING


Aquí usaremos multiprocesamiento para descargar imágenes por lotes con python.

Esto me ahorró mucho tiempo al descargar imágenes.


### Usage

```
python3 image_downloader.py <filename_with_urls_seperated_by_newline.txt> <num_of_process>
```

Esto leerá todas las URL en el archivo de texto y las descargará en una carpeta con el mismo nombre que el nombre del archivo.
num_of_process es opcional. (por defecto usa 10 procesos)


### Example

```
python3 image_downloader.py cats.txt
```

![cat images downloading](https://snipboard.io/VOXItq.jpg)

![cat image downloading](https://snipboard.io/6UgtE2.jpg)
