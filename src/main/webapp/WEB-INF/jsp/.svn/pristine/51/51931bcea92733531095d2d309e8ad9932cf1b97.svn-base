<?php

/**
 * This is the model class for table "ad_stats_book_report".
 *
 * The followings are the available columns in table 'ad_stats_book_report':
 * @property integer $id
 * @property string $ad_user_id
 * @property string $select_date
 * @property integer $book_cycle
 * @property string $data
 * @property integer $file_type
 * @property string $create_time
 * @property string $update_time
 * @property integer $status
 */
class AdStatsBookReport extends CActiveRecord
{
    const TYPE_MYSQL = 1;
    const TYPE_HADOOP = 2;
    const STATUS_START = 1;
    const STATUS_FINISH = 2;
    const BOOKCYCLE_SINGLE = 1;
    const BOOKCYCLE_DAY = 2;
    const BOOKCYCLE_WEEK = 3;
    const BOOKCYCLE_MONTH = 4;
    const SELECTDATE_YESTODAY = 1;
    const SELECTDATE_BEFOREYESTODAY = 2;
    const SELECTDATE_LAST7DAY = 7;
    const SELECTDATE_LASTWEEK = 9;
    const SELECTDATE_CURRENTMONTH = 10;
    const SELECTDATE_LASTMONTH = 8;
    const SELECTDATE_EASTMONTH =11;
    const FILETYPE_CSV = 1;
    const FILETYPE_TXT = 2;
	/**
	 * Returns the static model of the specified AR class.
	 * @return AdStatsBookReport the static model class
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
		return 'ad_stats_book_report';
	}

	/**
	 * @return array validation rules for model attributes.
	 */
	public function rules()
	{
		// NOTE: you should only define rules for those attributes that
		// will receive user inputs.
		return array(
			array('data, create_time', 'required'),
			array('book_cycle, file_type, status', 'numerical', 'integerOnly'=>true),
			array('ad_user_id', 'length', 'max'=>20),
			array('select_date', 'length', 'max'=>50),
			array('data', 'length', 'max'=>1024),
			array('update_time', 'safe'),
			// The following rule is used by search().
			// Please remove those attributes that should not be searched.
			array('id, ad_user_id, select_date, book_cycle, data, file_type, create_time, update_time, status', 'safe', 'on'=>'search'),
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
			'select_date' => 'Select Date',
			'book_cycle' => 'Book Cycle',
			'data' => 'Data',
			'file_type' => 'File Type',
			'create_time' => 'Create Time',
			'update_time' => 'Update Time',
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

		$criteria->compare('select_date',$this->select_date,true);

		$criteria->compare('book_cycle',$this->book_cycle);

		$criteria->compare('data',$this->data,true);

		$criteria->compare('file_type',$this->file_type);

		$criteria->compare('create_time',$this->create_time,true);

		$criteria->compare('update_time',$this->update_time,true);

		$criteria->compare('status',$this->status);

		return new CActiveDataProvider('AdStatsBookReport', array(
			'criteria'=>$criteria,
		));
	}
}
