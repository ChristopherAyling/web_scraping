import pandas as pd
from skimage import io
from tqdm import tqdm
from sklearn.externals.joblib import Parallel, delayed
import multiprocessing as mp
import plac

def download_image(url, path):
	try: 
		io.imsave(path, io.imread(url))
	except Exception as e:
		print(e)

def main(csv_path, img_folder, parallel: ('Execute in parrallel', 'flag', 'p')):
	error_count = 0
	csv = pd.read_csv(csv_path)
	print(f"{csv_path} contains {len(csv)} rows")

	urls = csv['URL'].values
	names = csv['name'].values if 'name' in csv.headers else list(range(len(urls)))
	manifest = zip(urls, names)

	if parallel:
		num_cores = mp.cpu_count()
		print(f"# == executing in parallel using {num_cores} cores == #")
		Parallel(n_jobs=num_cores)(delayed(download_image)(url, f"{img_folder}/{name}.jpg") for url, name in manifest)
	else:
		print(f"# == executing sequentially == #")
		for url, ID in tqdm(z, total=len(csv)):
			try:
				img = io.imread(url)
				io.imsave(f"{img_folder}/{ID}.jpg", img)
			except Exception as e:
				print(e)
				error_count += 1

	print(f"finishing downloading files from {csv_path} into {img_folder}.\n\tErrors: {error_count}")

if __name__ == "__main__":
	plac.call(main)