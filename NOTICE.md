# Third-party notices

The source code in this repository is licensed under [CC BY 4.0](LICENSE). When you install the optional `tribe` extra and run the inference path, additional third-party terms apply to the data and model artifacts pulled at runtime.

## TRIBE v2 (Meta FAIR)

Brain-encoding model weights. Licensed [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (non-commercial). Any cortical-activity arrays written into the output GeoParquet are derivatives of these weights and inherit the non-commercial restriction.

Source, https://github.com/facebookresearch/tribev2

## Llama 3.2 (Meta)

TRIBE v2 uses Llama 3.2-3B as its language tower. Subject to the [Llama 3.2 Community License](https://www.llama.com/llama3_2/license/) and Acceptable Use Policy. Acceptance is required before downloading weights from Hugging Face.

## Mapillary

Street-level imagery is fetched from the Mapillary Graph API. Image data and metadata are subject to Mapillary's [Terms of Use](https://www.mapillary.com/terms) and [API Terms](https://help.mapillary.com/hc/en-us/articles/115001770409). Mapillary content carried inside the cache shards or output Parquet remains under those terms.

## HCP MMP1 parcellation

The 360-region cortical parcellation used to summarize TRIBE outputs is from Glasser et al., 2016, distributed via the Human Connectome Project under the [HCP Data Use Terms](https://www.humanconnectome.org/study/hcp-young-adult/data-use-terms).

## fsaverage5 mesh

The 20484-vertex cortical surface mesh is from FreeSurfer, released under the [FreeSurfer Software License](https://surfer.nmr.mgh.harvard.edu/fswiki/FreeSurferSoftwareLicense).

## Python dependencies

Runtime libraries (DuckDB, httpx, PyArrow, NumPy, Typer, Pillow, imageio, huggingface_hub, tqdm, python-dotenv) are each released under their own permissive licenses (MIT, Apache 2.0, BSD). See each package's metadata for details.
