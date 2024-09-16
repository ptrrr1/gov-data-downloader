import aiohttp
import asyncio
import os
import io
import wget
import zipfile
import requests
from bs4 import BeautifulSoup
from datetime import datetime


class Downloader:
    def __init__(self, url, output_path, size=1024):
        self.url = url
        response = self.__get_url_response(url)
        if response is not None:
            self.response = response

        self.output_path = output_path
        if not os.path.exists(output_path):
            os.mkdir(output_path)

        self.size = size

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
        response = requests.get(url)

        if response.status_code != 200:
            print("Failed to load page.")
            print(f"Status Code: {self.response.status_code}")
            return None

        return response

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
            if response is not None:
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
                    print(url)
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
        async with aiohttp.ClientSession() as s:
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
            print(f"At: {f} Timestamp: {datetime.now().strftime("%H:%M:%S")}")

            zip_files = self.__get_zip_links(base_url)
            asyncio.run(self.__async_download(base_url, zip_files, path))
