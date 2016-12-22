<?php

/**
 * This is the model class for table "ad_stats_book_report_task".
 *
 * The followings are the available columns in table 'ad_stats_book_report_task':
 * @property integer $id
 * @property string $ad_user_id
 * @property string $start_date
 * @property string $end_date
 * @property string $data
 * @property integer $file_type
 * @property string $create_date
 * @property string $create_time
 * @property string $update_time
 * @property string $download_key
 * @property integer $type
 * @property integer $status
 */
class AdStatsBookReportTask extends CActiveRecord
{
    const STATUS_START = 1;
    const STATUS_FINISH = 2;
    const STATUS_FAILURE = -2;
	/**
	 * Returns the static model of the specified AR class.
	 * @return AdStatsBookReportTask the static model class
	 */
	public static function model($className=__CLASS__)
	{
		return parent::model($className);
	}
    
    public function getDbConnection()
    {
        return Yii::app()->db_book_report;
    }

	/**
	 * @return string the associated database table name
	 */
	public function tableName()
	{
		return 'ad_stats_book_report_task';
	}

	/**
	 * @return array validation rules for model attributes.
	 */
	public function rules()
	{
		// NOTE: you should only define rules for those attributes that
		// will receive user inputs.
		return array(
			array('start_date, end_date, data, create_date, create_time, update_time, download_key', 'required'),
			array('file_type, type, status', 'numerical', 'integerOnly'=>true),
			array('ad_user_id', 'length', 'max'=>20),
			array('data', 'length', 'max'=>1024),
			array('download_key', 'length', 'max'=>32),
			// The following rule is used by search().
			// Please remove those attributes that should not be searched.
			array('id, ad_user_id, start_date, end_date, data, file_type, create_date, create_time, update_time, download_key, type, status', 'safe', 'on'=>'search'),
		);
	}

	/**
	 * @return array relational rules.
	 */
	public function relations()
	{
		// NOTE: you may need to adjust the relation name and the related
		// class name for the relations automatically generated below.
		return array(
		);
	}

	/**
	 * @return array customized attribute labels (name=>label)
	 */
	public function attributeLabels()
	{
		return array(
			'id' => 'Id',
			'ad_user_id' => 'Ad User',
			'start_date' => 'Start Date',
			'end_date' => 'End Date',
			'data' => 'Data',
			'file_type' => 'File Type',
			'create_date' => 'Create Date',
			'create_time' => 'Create Time',
			'update_time' => 'Update Time',
			'download_key' => 'Download Key',
			'type' => 'Type',
			'status' => 'Status',
		);
	}

	/**
	 * Retrieves a list of models based on the current search/filter conditions.
	 * @return CActiveDataProvider the data provider that can return the models based on the search/filter conditions.
	 */
	public function search()
	{
		// Warning: Please modify the following code to remove attributes that
		// should not be searched.

		$criteria=new CDbCriteria;

		$criteria->compare('id',$this->id);

		$criteria->compare('ad_user_id',$this->ad_user_id,true);

		$criteria->compare('start_date',$this->start_date,true);

		$criteria->compare('end_date',$this->end_date,true);

		$criteria->compare('data',$this->data,true);

		$criteria->compare('file_type',$this->file_type);

		$criteria->compare('create_date',$this->create_date,true);

		$criteria->compare('create_time',$this->create_time,true);

		$criteria->compare('update_time',$this->update_time,true);

		$criteria->compare('download_key',$this->download_key,true);

		$criteria->compare('type',$this->type);

		$criteria->compare('status',$this->status);

		return new CActiveDataProvider('AdStatsBookReportTask', array(
			'criteria'=>$criteria,
		));
	}
}
