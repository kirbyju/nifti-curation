import os
import json
import pydicom
import SimpleITK as sitk

def process_dicom_series(base_folder):
    """
    Process DICOM files in the base folder recursively.
    Extract metadata and convert DICOM series to NIfTI.
    """
    nifti_folder = os.path.join(base_folder, "nifti")
    os.makedirs(nifti_folder, exist_ok=True)

    for root, _, files in os.walk(base_folder):
        dicom_files = [os.path.join(root, f) for f in files if f.lower().endswith(".dcm")]

        if not dicom_files:
            continue  # Skip if no DICOM files in this folder

        # Process metadata and convert to NIfTI
        metadata, common_metadata = extract_metadata(dicom_files)
        if not metadata or not common_metadata:
            print(f"Skipping directory {root}: Could not extract metadata.")
            continue

        # Generate filenames based on DICOM headers
        example_file = pydicom.dcmread(dicom_files[0])
        patient_id = example_file.PatientID
        modality = example_file.Modality
        series_instance_uid = example_file.SeriesInstanceUID

        base_filename = f"{patient_id}-{modality}-{series_instance_uid}"
        json_output_path = os.path.join(nifti_folder, f"{base_filename}.json")
        nifti_output_path = os.path.join(nifti_folder, f"{base_filename}.nii.gz")

        # Save metadata as JSON
        save_metadata_as_json(metadata, common_metadata, json_output_path)

        # Convert series to NIfTI with SimpleITK
        convert_to_nifti(root, nifti_output_path)

def extract_metadata(dicom_files):
    """
    Extract common and unique metadata from a list of DICOM files.
    """
    all_metadata = []
    common_metadata = {}

    # Read metadata from all files
    for file in dicom_files:
        ds = pydicom.dcmread(file, stop_before_pixels=True)
        file_metadata = {
            elem.tag: (elem.name, elem.value)
            for elem in ds
            if not elem.tag.is_private  # Exclude private tags
        }
        all_metadata.append(file_metadata)

    # Determine common metadata
    for key in all_metadata[0]:
        if all(file_metadata.get(key) == all_metadata[0].get(key) for file_metadata in all_metadata):
            common_metadata[key] = all_metadata[0][key]

    # Determine unique metadata
    unique_metadata = []
    for meta in all_metadata:
        unique_metadata.append(
            {key: value for key, value in meta.items() if key not in common_metadata}
        )

    # Debugging: Log identified metadata
    print("Common Metadata:", common_metadata)
    print("Unique Metadata Samples:", unique_metadata[:3])  # Show the first few

    return unique_metadata, common_metadata

def save_metadata_as_json(unique_metadata, common_metadata, output_path):
    """
    Save metadata as a JSON file, ensuring all values are JSON-serializable.
    """
    json_data = {
        "common_metadata": serialize_metadata(common_metadata),
        "unique_metadata": [serialize_metadata(meta) for meta in unique_metadata],
    }
    with open(output_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)

def serialize_metadata(metadata):
    """
    Recursively serialize DICOM metadata to ensure all values are JSON-serializable.
    """
    def serialize_value(value):
        if isinstance(value, pydicom.multival.MultiValue):
            return [serialize_value(v) for v in value]  # Recursively handle MultiValue
        elif isinstance(value, pydicom.sequence.Sequence):
            return [serialize_metadata({str(el.tag): (el.name, el.value) for el in item}) for item in value]  # Handle nested sequences
        elif isinstance(value, bytes):
            return value.decode(errors="ignore")  # Decode bytes to string
        elif isinstance(value, pydicom.valuerep.PersonName):
            return str(value)  # Convert PersonName to a string
        elif isinstance(value, (float, int, str)):
            return value  # Keep primitive types as-is
        return str(value)  # Fallback: convert any other type to string

    return {str(key): (value[0], serialize_value(value[1])) for key, value in metadata.items()}

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
        print(f"Converted DICOM to NIfTI: {output_path}")
    except Exception as e:
        print(f"Error converting DICOM to NIfTI: {e}")

if __name__ == "__main__":
    base_folder = input("Enter the path to the base folder: ").strip()
    process_dicom_series(base_folder)
