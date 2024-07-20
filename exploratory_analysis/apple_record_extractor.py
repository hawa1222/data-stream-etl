import pandas as pd

from constants import Apple, FileDirectory
from utility.file_manager import FileManager
from utility.log_manager import setup_logging

logger = setup_logging()

RECORD_TYPE = {
    "HKQuantityTypeIdentifierActiveEnergyBurned": 737331,
    "HKQuantityTypeIdentifierBasalEnergyBurned": 457894,
    "HKQuantityTypeIdentifierHeartRate": 381050,
    "HKQuantityTypeIdentifierDistanceWalkingRunning": 349033,
    "HKQuantityTypeIdentifierStepCount": 295037,
    "HKQuantityTypeIdentifierPhysicalEffort": 141235,
    "HKQuantityTypeIdentifierRunningSpeed": 139794,
    "HKQuantityTypeIdentifierRunningPower": 135019,
    "HKQuantityTypeIdentifierRunningStrideLength": 65789,
    "HKQuantityTypeIdentifierRunningVerticalOscillation": 65523,
    "HKQuantityTypeIdentifierRunningGroundContactTime": 65478,
    "HKQuantityTypeIdentifierAppleExerciseTime": 59602,
    "HKQuantityTypeIdentifierAppleStandTime": 40494,
    "HKQuantityTypeIdentifierRespiratoryRate": 30010,
    "HKCategoryTypeIdentifierSleepAnalysis": 23117,
    "HKCategoryTypeIdentifierAppleStandHour": 15414,
    "HKQuantityTypeIdentifierOxygenSaturation": 12466,
    "HKQuantityTypeIdentifierFlightsClimbed": 12159,
    "HKQuantityTypeIdentifierWalkingStepLength": 9712,
    "HKQuantityTypeIdentifierWalkingSpeed": 9712,
    "HKQuantityTypeIdentifierWalkingDoubleSupportPercentage": 8866,
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": 6867,
    "HKQuantityTypeIdentifierBloodGlucose": 5917,
    "HKQuantityTypeIdentifierWalkingAsymmetryPercentage": 5026,
    "HKQuantityTypeIdentifierEnvironmentalAudioExposure": 3836,
    "HKQuantityTypeIdentifierStairAscentSpeed": 3269,
    "HKCategoryTypeIdentifierHandwashingEvent": 2361,
    "HKQuantityTypeIdentifierStairDescentSpeed": 1583,
    "HKQuantityTypeIdentifierTimeInDaylight": 1314,
    "HKCategoryTypeIdentifierLowHeartRateEvent": 1025,
    "HKQuantityTypeIdentifierRestingHeartRate": 711,
    "HKQuantityTypeIdentifierWalkingHeartRateAverage": 707,
    "HKQuantityTypeIdentifierVO2Max": 653,
    "HKQuantityTypeIdentifierHeartRateRecoveryOneMinute": 489,
    "HKCategoryTypeIdentifierMindfulSession": 392,
    "HKQuantityTypeIdentifierHeadphoneAudioExposure": 286,
    "HKQuantityTypeIdentifierDietaryEnergyConsumed": 124,
    "HKQuantityTypeIdentifierDietaryProtein": 119,
    "HKQuantityTypeIdentifierDietaryCarbohydrates": 118,
    "HKQuantityTypeIdentifierDietaryFatSaturated": 106,
    "HKQuantityTypeIdentifierDietarySugar": 100,
    "HKQuantityTypeIdentifierDietaryFatTotal": 96,
    "HKQuantityTypeIdentifierDietarySodium": 95,
    "HKQuantityTypeIdentifierDietaryFiber": 88,
    "HKQuantityTypeIdentifierSixMinuteWalkTestDistance": 81,
    "HKQuantityTypeIdentifierDietaryPotassium": 67,
    "HKQuantityTypeIdentifierBodyMass": 53,
    "HKQuantityTypeIdentifierAppleWalkingSteadiness": 41,
    "HKQuantityTypeIdentifierDietaryWater": 20,
    "HKQuantityTypeIdentifierDietaryCholesterol": 20,
    "HKQuantityTypeIdentifierBodyMassIndex": 18,
    "HKCategoryTypeIdentifierAudioExposureEvent": 7,
    "HKDataTypeSleepDurationGoal": 2,
}


def extract_sleep_data(root, record_type):
    logger.info("Extracting sleep data from Apple Health XML...")

    try:
        apple_records = []
        for record in root.findall(".//Record"):
            if record.get("type") == record_type:
                apple_record = record.attrib
                apple_records.append(apple_record)

        df = pd.DataFrame(apple_records)

        logger.info(
            f"Successfully extracted {len(df)} sleep records with {len(df.columns)} fields."
        )
        return df

    except Exception as e:
        logger.error(f"Error extracting {record_type} data: {str(e)}")
        raise


def main():
    record_type = "HKCategoryTypeIdentifierMindfulSession"

    logger.info("Starting Apple {record_type} data extraction...")

    file_manager = FileManager()

    try:
        tree = file_manager.load_file(FileDirectory.SOURCE_DATA_PATH, Apple.XML_DATA, "xml")
        root = tree.getroot()

        apple_df = extract_sleep_data(root, record_type)

        if not apple_df.empty:
            file_manager.save_file(FileDirectory.RAW_DATA_PATH, record_type, apple_df)
        else:
            logger.warning("No {record_type} data extracted. No file saved.")

    except Exception as e:
        logger.error(f"Error extracting Apple {record_type} data: {str(e)}")


if __name__ == "__main__":
    main()
