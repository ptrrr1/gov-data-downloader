import aiohttp
import asyncio
import os
import io
import wget
import zipfile
import requests
from bs4 import BeautifulSoup
from datetime import datetime

#
# Async download is only recommended for folders, as of now.
# Though it works without problems most of the time, it is very prone to erros
# Normal download is ideal for every other use case. The only problem is that:
# 1. It is slower compared to async;
# 2. Current method loads evey file in memory and then extracts
# 3. Does not compare before downloading, it may try to redownload everything (TODO)
#

class Downloader:
    def __init__(self, url, output_path):
        self.url = url
        self.response = self.__get_url_response(url)
        self.output_path = output_path
        try:
            if not os.path.exists(output_path):
                os.mkdir(output_path)
        except Exception as e:
            print(f"Failed to create dir {e}")

    def __check_diff(self, url, file_name):
        if not os.path.isfile(file_name):
            # Hasn't been downloaded yet
            return True

        response = requests.head(url)
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            # Different sizes
            return True

        # Same file
        return False

    def __get_url_response(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as e:
            print(f"Some error occurred: {e}")

    def __get_folders_link(self):
        soup = BeautifulSoup(self.response.text, "html.parser")
        links = [
                    a['href'] for a in soup.find_all('a', href=True)
                    if a['href'].endswith('/')  # Assumes they all end with '/'
                ]

        # First one is not one of the folders, it is the parent folder
        return links[1:]

    def __get_zip_links(self, url):
        response = self.__get_url_response(url)
        soup = BeautifulSoup(response.text, "html.parser")
        links = [
                    a['href'] for a in soup.find_all('a', href=True)
                    if a['href'].endswith('.zip')
                ]

        return links

    def __download(self, zip_files, url, path):
        for z in zip_files:
            full_zip_url = url + z
            file_name = z.replace("%20", " ")
            response = self.__get_url_response(full_zip_url)
            try:
                # Stores in memory then extracts
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    z.extractall(path)
                print(f"Downloaded and Extracted {file_name}")
            except Exception as e:
                print(f"Failed to download {file_name} - {e}")

    def download(self):
        zip_files = self.__get_zip_links(self.url)
        self.__download(zip_files, self.url, self.output_path)

    # Not truly recursive as it only searches one level and then looks for zips
    # TODO: Make actually recursive
    def recursive_download(self, start_range=None, end_range=None):
        folders = self.__get_folders_link()

        for f in folders[start_range:end_range]:
            # f[:-1] is to remove '/' at the end
            path = os.path.join(self.output_path, f[:-1])
            if not os.path.exists(path):
                os.mkdir(path)

            full_url = self.url + f
            print(f"At: {f} Timestamp: {datetime.now().strftime("%H:%M:%S")}")

            zip_files = self.__get_zip_links(full_url)
            self.__download(zip_files, full_url, path)

    async def __async_download_zip(self, url, session, file_name, folder_path):
        file_name = file_name.replace("%20", " ")
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    print(f"Failed to download {file_name}")
                    return

                # If it hasn't been dowloaded yet, then download
                # Since it compares the zip folder,
                # if it is deleted then it'll try again
                path = os.path.join(folder_path, file_name)
                if self.__check_diff(url, path):
                    # Using Wget to download,
                    # tried other methods, this seemed more reliable
                    wget.download(url, out=path)
                    # print(f"Downloaded {file_name}")

                    # Extract
                    try:
                        with zipfile.ZipFile(path, 'r') as z:
                            z.extractall(folder_path)

                        print(f"Downloaded and Extracted {file_name}")

                    except Exception as e:
                        print(f"Failed to extract {file_name} - {e}")

        except Exception as e:
            print(f"Error downloading {file_name} - {e}")

    async def __async_download(self, base_url, zip_url, folder_path):
        async with aiohttp.ClientSession(trust_env=True) as s:
            tasks = [self.__async_download_zip(base_url + z, s, z, folder_path) for z in zip_url]
            await asyncio.gather(*tasks)

    def async_download(self):
        zip_files = self.__get_zip_links(self.url)
        asyncio.run(self.__async_download(self.url, zip_files, self.output_path))

    # Not truly recursive as it only searches one level and then looks for zips
    # TODO: Make actually recursive
    def async_recursive_download(self, start_range=None, end_range=None):
        folders = self.__get_folders_link()

        for f in folders[start_range:end_range]:
            # f[:-1] is to remove '/' at the end
            path = os.path.join(self.output_path, f[:-1])
            if not os.path.exists(path):
                os.mkdir(path)

            base_url = self.url + f
            print(f"At: {f} Start Timestamp: {datetime.now().strftime("%H:%M:%S")}")

            zip_files = self.__get_zip_links(base_url)
            asyncio.run(self.__async_download(base_url, zip_files, path))

            print(f"At: {f} End Timestamp: {datetime.now().strftime("%H:%M:%S")}")
