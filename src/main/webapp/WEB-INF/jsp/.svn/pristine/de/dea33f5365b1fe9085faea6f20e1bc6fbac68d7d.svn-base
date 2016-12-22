<?php

/**
 * This is the model class for table "ad_stats_keyword".
 *
 * The followings are the available columns in table 'ad_stats_keyword':
 * @property integer $id
 * @property integer $clicks
 * @property integer $views
 * @property string $costs
 * @property integer $trans
 * @property integer $ad_group_id
 * @property integer $ad_plan_id
 * @property integer $ad_keyword_id
 * @property integer $ad_user_id
 * @property string $create_date
 * @property string $keyword
 */
class AdKeyword extends CActiveRecord
{
    static $db;
	/**
	 * Returns the static model of the specified AR class.
	 * @return AdKeyword the static model class
	 */
	public static function model($className=__CLASS__)
	{
		return parent::model($className);
	}

	/**
	 * @return string the associated database table name
	 */
	public function tableName()
	{
		return 'ad_stats_keyword';
	}

	/**
	 * @return array validation rules for model attributes.
	 */
	public function rules()
	{
		// NOTE: you should only define rules for those attributes that
		// will receive user inputs.
		return array(
			array('ad_group_id, ad_plan_id, ad_user_id, create_date', 'required'),
			array('clicks, views, trans, ad_group_id, ad_plan_id, ad_keyword_id, ad_user_id', 'numerical', 'integerOnly'=>true),
			array('costs', 'length', 'max'=>12),
			array('keyword', 'length', 'max'=>32),
			// The following rule is used by search().
			// Please remove those attributes that should not be searched.
			array('id, clicks, views, costs, trans, ad_group_id, ad_plan_id, ad_keyword_id, ad_user_id, create_date, keyword', 'safe', 'on'=>'search'),
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
			'clicks' => 'Clicks',
			'views' => 'Views',
			'costs' => 'Costs',
			'trans' => 'Trans',
			'ad_group_id' => 'Ad Group',
			'ad_plan_id' => 'Ad Plan',
			'ad_keyword_id' => 'Ad Keyword',
			'ad_user_id' => 'Ad User',
			'create_date' => 'Create Date',
			'keyword' => 'Keyword',
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

		$criteria->compare('clicks',$this->clicks);

		$criteria->compare('views',$this->views);

		$criteria->compare('costs',$this->costs,true);

		$criteria->compare('trans',$this->trans);

		$criteria->compare('ad_group_id',$this->ad_group_id);

		$criteria->compare('ad_plan_id',$this->ad_plan_id);

		$criteria->compare('ad_keyword_id',$this->ad_keyword_id);

		$criteria->compare('ad_user_id',$this->ad_user_id);

		$criteria->compare('create_date',$this->create_date,true);

		$criteria->compare('keyword',$this->keyword,true);

		return new CActiveDataProvider('AdKeyword', array(
			'criteria'=>$criteria,
		));
	}

    public function getDbConnection()
    {
        if(self::$db!==null)
            return self::$db;
        else{
            self::$db=Yii::app()->db_stats;
            if(self::$db instanceof CDbConnection)
            {
                self::$db->setActive(true);
                return self::$db;
            }
            else
                throw new CDbException(Yii::t('yii','...'));
        }
    }
}
