import os
import json
import logging
import pydicom
import SimpleITK as sitk
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_dicom_series(base_folder):
    """
    Process DICOM files in the base folder recursively.
    Extract metadata and convert DICOM series to NIfTI.
    """
    nifti_folder = os.path.join(base_folder, "nifti")
    os.makedirs(nifti_folder, exist_ok=True)

    with ThreadPoolExecutor() as executor:
        futures = []
        for root, _, files in os.walk(base_folder):
            dicom_files = [os.path.join(root, f) for f in files if f.lower().endswith(".dcm")]
            if dicom_files:
                futures.append(executor.submit(process_series, dicom_files, nifti_folder))

        for future in futures:
            future.result()  # Wait for all tasks to complete

def process_series(dicom_files, nifti_folder):
    """
    Process a single DICOM series.
    """
    try:
        # Extract metadata in DICOM JSON format
        series_metadata = extract_metadata(dicom_files)
        if not series_metadata:
            logging.warning("Skipping series: Could not extract metadata.")
            return

        # Generate output filenames
        example_file = pydicom.dcmread(dicom_files[0])
        base_filename = generate_filename(example_file)
        json_output_path = os.path.join(nifti_folder, f"{base_filename}.json")
        nifti_output_path = os.path.join(nifti_folder, f"{base_filename}.nii.gz")

        # Save metadata as JSON
        save_metadata_as_json(series_metadata, json_output_path)

        # Convert DICOM to NIfTI
        convert_to_nifti(os.path.dirname(dicom_files[0]), nifti_output_path)
    except Exception as e:
        logging.error(f"Error processing series: {e}", exc_info=True)

def extract_metadata(dicom_files):
    """
    Extract metadata from a list of DICOM files and represent it in DICOM JSON format.
    """
    series_metadata = []
    for file in dicom_files:
        ds = pydicom.dcmread(file, stop_before_pixels=True)
        slice_metadata = serialize_metadata({
            elem.tag: (elem.name, elem.value)
            for elem in ds
            if not elem.tag.is_private  # Exclude private tags
        })
        series_metadata.append(slice_metadata)

    return series_metadata

def serialize_metadata(metadata):
    """
    Serialize DICOM metadata in DICOM JSON format.
    """
    serialized = {}
    for key, value in metadata.items():
        tag = format(key, "08X")  # Convert tag to 8-character hexadecimal string
        name, val = value  # (name, value) tuple
        vr = pydicom.datadict.dictionary_VR(key)  # Get VR from DICOM dictionary

        # Handle different value types
        if isinstance(val, pydicom.multival.MultiValue):
            serialized[tag] = {"vr": vr, "Value": [str(v) for v in val]}
        elif isinstance(val, pydicom.sequence.Sequence):
            serialized[tag] = {"vr": vr, "Value": [serialize_metadata({el.tag: (el.name, el.value) for el in item}) for item in val]}
        elif isinstance(val, bytes):
            serialized[tag] = {"vr": vr, "Value": val.decode(errors="ignore")}
        elif isinstance(val, (float, int, str)):
            serialized[tag] = {"vr": vr, "Value": val}
        else:
            serialized[tag] = {"vr": vr, "Value": str(val)}

    return serialized

def save_metadata_as_json(series_metadata, output_path):
    """
    Save metadata as a JSON file in DICOM JSON format.
    """
    with open(output_path, "w") as json_file:
        json.dump(series_metadata, json_file, indent=4)

def convert_to_nifti(dicom_dir, output_path):
    """
    Convert a DICOM series to a NIfTI file using SimpleITK.
    """
    try:
        reader = sitk.ImageSeriesReader()
        dicom_names = reader.GetGDCMSeriesFileNames(dicom_dir)
        reader.SetFileNames(dicom_names)
        image = reader.Execute()

        sitk.WriteImage(image, output_path)
        logging.info(f"Converted DICOM to NIfTI: {output_path}")
    except Exception as e:
        logging.error(f"Error converting DICOM to NIfTI: {e}", exc_info=True)

def generate_filename(example_file):
    """
    Generate a filename based on DICOM headers, with fallbacks for missing fields.
    Sanitize the SeriesInstanceUID for use in filenames.
    """
    patient_id = getattr(example_file, "PatientID", "UNKNOWN_PATIENT")
    modality = getattr(example_file, "Modality", "UNKNOWN_MODALITY")
    series_uid = getattr(example_file, "SeriesInstanceUID", "UNKNOWN_SERIES")

    # Sanitize the SeriesInstanceUID for filenames
    sanitized_series_uid = series_uid.replace(".", "_")  # Replace periods with underscores

    # Generate the filename
    filename = f"{patient_id}-{modality}-{sanitized_series_uid}"
    filename = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in filename)  # Further sanitization
    return filename

if __name__ == "__main__":
    base_folder = input("Enter the path to the base folder: ").strip()
    process_dicom_series(base_folder)
